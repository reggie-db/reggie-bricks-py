"""Helpers for building PydanticAI agents backed by Databricks endpoints.

This module centralizes agent creation, OpenAI-compatible Databricks serving
clients, and OTLP-based tracing setup for PydanticAI via Logfire.

Tracing is sent to the workspace's MLflow OTLP traces endpoint
(``/api/2.0/mlflow/otlp/v1/traces``) so agent runs land alongside other MLflow
experiments. Authentication headers are refreshed on every export so OAuth
tokens that rotate (e.g. inside a Databricks App) stay valid for long-running
agents.

The authenticated httpx transport that backs the OpenAI-compatible clients
lives in ``dbx_tools.clients.api``. The :func:`http_client` helper below
remains as a deprecated shim for backwards compatibility.
"""

import functools
import os
import warnings
from typing import Any, Literal

import httpx
import logfire
from databricks.sdk import WorkspaceClient
from dbx_core import objects, projects, strs
from dbx_tools import clients, configs, experiments
from lfp_logging import logs
from openai import AsyncClient
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor
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
    ``instrument="auto"``, this helper configures :func:`_auto_instrument`
    (Logfire-backed OTLP tracing to the workspace's MLflow OTLP endpoint) and
    turns on PydanticAI instrumentation unless an explicit ``output_type`` was
    supplied (which disables tracing because structured-output agents are not
    compatible with PydanticAI's OTEL instrumentation).

    Args:
        model_name: Optional Databricks model serving endpoint name. When
            omitted, the default large model is used.
        instrument: Instrumentation configuration. When set to ``"auto"``, this
            helper enables OTLP tracing and uses ``instrument=True`` unless an
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
def _auto_instrument() -> None:
    """Configure Logfire-backed OTLP tracing for PydanticAI agents (once per process).

    Spans produced by PydanticAI's built-in OTEL instrumentation are exported
    to the workspace's MLflow OTLP traces endpoint
    (``/api/2.0/mlflow/otlp/v1/traces``) so agent runs, tool invocations, and
    model calls show up alongside other MLflow experiments.

    The target experiment is sourced from ``MLFLOW_EXPERIMENT_ID`` first, then
    ``MLFLOW_EXPERIMENT_NAME``, and finally :func:`dbx_tools.experiments.get`'s
    project default. Bearer auth headers are re-resolved per export via
    :class:`_DatabricksMlflowOtlpExporter` so OAuth tokens that rotate
    (e.g. inside Databricks Apps) stay valid for long-running agents.
    """
    workspace_client = clients.workspace_client()
    host = workspace_client.config.host
    if not host:
        raise RuntimeError(
            "WorkspaceClient has no host configured; OTLP tracing requires a host"
        )
    experiment_id = _resolve_experiment_id(workspace_client)
    endpoint = f"{host.rstrip('/')}/api/2.0/mlflow/otlp/v1/traces"
    exporter = _DatabricksMlflowOtlpExporter(
        endpoint=endpoint,
        workspace_client=workspace_client,
        experiment_id=experiment_id,
    )
    logfire.configure(
        send_to_logfire=False,
        service_name=projects.root_project_name(),
        additional_span_processors=[BatchSpanProcessor(exporter)],
    )
    # PydanticAI's Agent(instrument=True) already emits OTEL spans; this adds
    # logfire's pydantic-ai-specific instrumentation (tool args, prompt args,
    # etc.) on top of them.
    logfire.instrument_pydantic_ai()
    LOG.info(
        f"Logfire OTLP tracing configured - "
        f"config_profile:{configs.profile()} endpoint:{endpoint} experiment_id:{experiment_id}"
    )


def _resolve_experiment_id(workspace_client: WorkspaceClient) -> str:
    """Resolve the MLflow experiment id used as the OTLP trace destination.

    Priority order:

    1. ``MLFLOW_EXPERIMENT_ID`` env var (raw id, used as-is).
    2. ``MLFLOW_EXPERIMENT_NAME`` env var (looked up / created via
       :func:`dbx_tools.experiments.get`).
    3. :func:`dbx_tools.experiments.get` with no lookup, which falls back to
       the running app name or the root project name.
    """
    if experiment_id := os.environ.get("MLFLOW_EXPERIMENT_ID"):
        return experiment_id
    experiment_name = os.environ.get("MLFLOW_EXPERIMENT_NAME")
    if experiment_name:
        experiment = experiments.get(
            experiment_lookup=experiment_name,
            workspace_client=workspace_client,
        )
    else:
        experiment = experiments.get(workspace_client=workspace_client)
    if not experiment.experiment_id:
        raise RuntimeError(
            f"Could not resolve an MLflow experiment id for OTLP tracing: {experiment}"
        )
    return experiment.experiment_id


class _DatabricksMlflowOtlpExporter(OTLPSpanExporter):
    """OTLP/HTTP span exporter that re-authenticates per batch against Databricks.

    The base ``OTLPSpanExporter`` accepts headers only at construction time,
    which prevents OAuth token refresh from being picked up. Overriding
    :meth:`_export` lets us refresh the bearer token (and re-stamp the
    ``x-mlflow-experiment-id`` header MLflow uses to route the spans) on
    every batch.
    """

    def __init__(
        self,
        endpoint: str,
        workspace_client: WorkspaceClient,
        experiment_id: str,
    ):
        super().__init__(endpoint=endpoint)
        self._workspace_client = workspace_client
        self._experiment_id = experiment_id

    def _export(self, serialized_data: bytes, timeout_sec: float | None = None):
        # ``config.authenticate()`` returns {"Authorization": "Bearer ..."}; calling
        # it on every export rides through OAuth rotation transparently.
        auth_headers = self._workspace_client.config.authenticate()
        self._session.headers["Authorization"] = auth_headers["Authorization"]
        self._session.headers["x-mlflow-experiment-id"] = self._experiment_id
        return super()._export(serialized_data, timeout_sec)


def client(workspace_client: WorkspaceClient | None = None) -> AsyncClient:
    """Return an OpenAI-compatible async client backed by Databricks serving.

    Passing ``workspace_client`` creates a client bound to that workspace.
    Omitting it reuses the cached default client stack. The underlying httpx
    transport is built via :func:`dbx_tools.clients.api`.
    """
    return _client(workspace_client) if workspace_client else _client_default()


def http_client(workspace_client: WorkspaceClient | None = None) -> httpx.AsyncClient:
    """Return an authenticated HTTP client for Databricks REST endpoints.

    .. deprecated::
        Use :func:`dbx_tools.clients.api` directly. This shim is kept to
        avoid breaking existing imports.
    """
    _http_client_warning()
    return clients.api(workspace_client)


@functools.cache
def _http_client_warning():
    warnings.warn(
        "dbx_ai.agents.http_client is deprecated; use dbx_tools.clients.api(...) instead.",
        DeprecationWarning,
        stacklevel=2,
    )


def model(
    model_name: str | None = None, workspace_client: WorkspaceClient | None = None
) -> Model:
    """Return a PydanticAI model wrapper for a Databricks serving endpoint."""
    if not model_name:
        model_name = models.large()
    provider = OpenAIProvider(openai_client=client(workspace_client))
    # noinspection PyTypeChecker
    return OpenAIChatModel(model_name=model_name, provider=provider)


@functools.cache
def large() -> Agent[None, str]:
    """Return a cached agent configured with the default large model."""
    return create(models.large())


@functools.cache
def small() -> Agent[None, str]:
    """Return a cached agent configured with the default small model."""
    return create(models.small())


def _client(workspace_client: WorkspaceClient) -> AsyncClient:
    """Build an async OpenAI client for an explicit Databricks workspace."""
    return AsyncClient(
        base_url=workspace_client.config.host + "/serving-endpoints",
        api_key="no-token",  # placeholder to pass openai-python validation; auth happens via http_client
        http_client=clients.api(workspace_client),
    )


@functools.cache
def _client_default() -> AsyncClient:
    """Return the cached default OpenAI-compatible Databricks client."""
    return AsyncClient(
        base_url=configs.get().host + "/serving-endpoints",
        api_key="no-token",  # placeholder to pass openai-python validation; auth happens via http_client
        http_client=clients.api(),
    )


async def main() -> None:
    """Run a local streaming tool-use example agent."""
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
