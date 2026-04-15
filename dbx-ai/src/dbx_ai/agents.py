"""Helpers for building PydanticAI agents backed by Databricks endpoints.

This module centralizes agent creation, OpenAI-compatible Databricks serving
clients, and optional MLflow autologging setup for PydanticAI.
"""

import functools
import os
from typing import Any, Literal

import httpx
import mlflow
import mlflow.pydantic_ai as pydantic_ai_mlflow
from databricks.sdk import WorkspaceClient
from dbx_core import objects, projects, strs
from dbx_tools import clients, configs, experiments
from lfp_logging import logs
from openai import AsyncClient
from pydantic_ai import Agent
from pydantic_ai.models import Model
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider

from dbx_ai import models

LOG = logs.logger()

_DEFAULT_INSTRUCTIONS = strs.trim("""
Do not include emojis, em dashes, or en dashes in responses.
If dashes are needed, use a standard hyphen (-) instead.
""")



def create(
    model_name: str | None = None,
    instrument: Literal[True, False, "auto"] = "auto",
    workspace_client: WorkspaceClient | None = None,
    **kwargs: Any,
) -> Agent[Any, Any]:
    """Create a configured PydanticAI agent.

    The created agent always includes baseline response instructions. When
    ``instrument="auto"``, this helper enables MLflow autologging and turns on
    PydanticAI instrumentation unless an explicit ``output_type`` was supplied.

    Args:
        model_name: Optional Databricks model serving endpoint name. When
            omitted, the default large model is used.
        instrument: Instrumentation configuration. When set to ``"auto"``, this
            helper enables autologging and uses ``instrument=True`` unless an
            explicit ``output_type`` disables that default path.
        workspace_client: Optional Databricks workspace client used for serving
            requests. When omitted, cached default clients are used.
        **kwargs: Additional keyword arguments forwarded to ``pydantic_ai.Agent``.

    Returns:
        A configured ``Agent`` instance.
    """

    instructions = [_DEFAULT_INSTRUCTIONS]
    if kwargs_instructions := strs.trim(kwargs.pop("instructions", None)):
        for instruction in objects.to_list(kwargs_instructions, flatten=True):
            if instruction := strs.trim(instruction):
                instructions.append(instruction)
    if "auto" == instrument:
        output_type_kwarg = "output_type"
        output_type = kwargs.get(output_type_kwarg, None)
        if output_type is None:
            _auto_instrument()
            instrument = True
        else:
            instrument = False
    kwargs["instrument"] = instrument
    return Agent(
        model=model(model_name=model_name, workspace_client=workspace_client),
        instructions=instructions,
        **kwargs,
    )



@functools.cache
def _auto_instrument():
    """Configure MLflow PydanticAI autologging once per process.

    The tracking URI is set to Databricks when not already configured. The
    target experiment is sourced from ``MLFLOW_EXPERIMENT_ID`` first, then
    ``MLFLOW_EXPERIMENT_NAME``, and finally the root project name.
    """
    config_profile = configs.profile()
    if not mlflow.is_tracking_uri_set():
        mlflow.set_tracking_uri("databricks")
    experiment_id = os.environ.get("MLFLOW_EXPERIMENT_ID", None)
    if experiment_id:
        experiment_name = None
    else:
        experiment_name = os.environ.get("MLFLOW_EXPERIMENT_NAME", None)
        if not experiment_name:
            experiment_id = experiments.get().experiment_id
            mlflow.set_experiment(experiment_id=experiment_id)
    LOG.info(
        "MLflow auto instrument - config_profile:%s tracking_uri:%s experiment_id:%s experiment_name:%s",
        config_profile,
        mlflow.get_tracking_uri(),
        experiment_id,
        experiment_name,
    )
    pydantic_ai_mlflow.autolog()
    _patch_mlflow_circular_ref()


@functools.cache
def _patch_mlflow_circular_ref() -> None:
    """Patch MLflow's span serializer to handle circular references.

    mlflow.pydantic_ai.autolog() wraps every model request and calls
    json.dumps on the ModelRequest inputs. Tool closures registered on an
    agent capture the agent itself, creating a circular reference that raises
    ValueError at serialization time. This patch retries with
    check_circular=False and falls back to a sentinel string rather than
    letting the error propagate and kill the streaming response.
    """
    import json as _json

    try:
        import mlflow.tracing.utils as _mlu
        from mlflow.tracing.utils import TraceJSONEncoder

        _orig = _mlu.dump_span_attribute_value

        def _safe_dump(value, **kw):  # type: ignore[override]
            try:
                return _orig(value, **kw)
            except (ValueError, TypeError):
                try:
                    return _json.dumps(
                        value,
                        cls=TraceJSONEncoder,
                        ensure_ascii=False,
                        check_circular=False,
                    )
                except Exception:
                    return '"[unserializable span input]"'

        _mlu.dump_span_attribute_value = _safe_dump
        # Patch the canonical module so future imports get the safe version.
        _mlu.dump_span_attribute_value = _safe_dump
        # Patch the already-bound name inside span.py's module namespace,
        # which holds a direct import reference set at module load time.
        # setattr used intentionally - patching an external untyped module namespace.
        import mlflow.entities.span as _span_mod
        if hasattr(_span_mod, "dump_span_attribute_value"):
            setattr(_span_mod, "dump_span_attribute_value", _safe_dump)

        LOG.debug("MLflow circular-reference guard installed")
    except Exception:
        LOG.warning("MLflow circular-reference guard could not be installed")

def client(workspace_client: WorkspaceClient | None = None) -> AsyncClient:
    """Return an OpenAI-compatible async client backed by Databricks serving.

    Passing ``workspace_client`` creates a client bound to that workspace.
    Omitting it reuses the cached default client stack.
    """
    return _client(workspace_client) if workspace_client else _client_default()


def _client(workspace_client: WorkspaceClient) -> AsyncClient:
    """Build an async OpenAI client for an explicit Databricks workspace."""
    hclient = http_client(workspace_client)
    client_params = {
        "base_url": workspace_client.config.host + "/serving-endpoints",
        "api_key": "no-token",  # Passing in a placeholder to pass validations, this will not be used
        "http_client": hclient,
    }
    return AsyncClient(**client_params)


@functools.cache
def _client_default() -> AsyncClient:
    """Return the cached default OpenAI-compatible Databricks client."""
    client_params = {
        "base_url": configs.get().host + "/serving-endpoints",
        "api_key": "no-token",  # Passing in a placeholder to pass validations, this will not be used
        "http_client": _http_client_default(),
    }
    return AsyncClient(**client_params)


@functools.cache
def large() -> Agent[None, str]:
    return create(models.large())


@functools.cache
def small() -> Agent[None, str]:
    return create(models.small())


def model(
    model_name: str | None = None, workspace_client: WorkspaceClient | None = None
) -> Model:
    """Return a PydanticAI model wrapper for a Databricks serving endpoint."""
    if not model_name:
        model_name = models.large()
    provider = OpenAIProvider(openai_client=client(workspace_client))
    # noinspection PyTypeChecker
    return OpenAIChatModel(model_name=model_name, provider=provider)


def http_client(workspace_client: WorkspaceClient | None = None) -> httpx.AsyncClient:
    """Return an authenticated HTTP client for Databricks serving requests.

    Passing ``workspace_client`` creates a client bound to that workspace.
    Omitting it reuses the cached default HTTP client.
    """
    return (
        _http_client_default()
        if workspace_client is None
        else _http_client(workspace_client)
    )


def _http_client(workspace_client: WorkspaceClient) -> httpx.AsyncClient:
    """Create an authenticated HTTP client for Databricks serving endpoints."""

    class AsyncBearerAuth(httpx.Auth):
        def __init__(self, header_fn):
            self._header_fn = header_fn

        async def async_auth_flow(self, request: httpx.Request):
            # Databricks SDK authentication is synchronous but returns the
            # authorization headers needed for each outgoing request.
            auth_headers = self._header_fn()
            request.headers["Authorization"] = auth_headers["Authorization"]
            yield request

    # noinspection PyProtectedMember
    bearer_auth = AsyncBearerAuth(
        workspace_client.serving_endpoints._api._cfg.authenticate
    )
    timeout = httpx.Timeout(connect=10.0, read=300.0, write=30.0, pool=30.0)
    try:
        import h2  # noqa: F401  # pyright: ignore[reportMissingImports]

        http2 = True
    except Exception:
        http2 = False
    return httpx.AsyncClient(
        auth=bearer_auth,
        timeout=timeout,
        http2=http2,
    )


@functools.cache
def _http_client_default() -> httpx.AsyncClient:
    """Return the cached default authenticated HTTP client."""
    return _http_client(clients.workspace_client())


async def main():
    from pydantic_ai import RunContext

    calculator_agent = create(
        system_prompt="You are a calculator assistant. Use the tools to perform calculations.",
    )

    @calculator_agent.tool
    def add(ctx: RunContext[None], a: float, b: float) -> float:
        """Add two numbers together."""
        return a + b

    @calculator_agent.tool
    def multiply(ctx: RunContext[None], a: float, b: float) -> float:
        """Multiply two numbers together."""
        return a * b

    @calculator_agent.tool
    def subtract(ctx: RunContext[None], a: float, b: float) -> float:
        """Subtract b from a."""
        return a - b

    print("Streaming calculation response:")
    async with calculator_agent.run_stream(
        "Calculate (5 + 3) * 2 - 1. Show your work step by step."
    ) as response:
        async for chunk in response.stream_text(delta=True):
            print(chunk, end="", flush=True)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
