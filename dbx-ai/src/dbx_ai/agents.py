import functools
import os
from typing import Any

import httpx
from databricks.sdk import WorkspaceClient
from dbx_core import objects, strs
from dbx_tools import clients, configs, experiments
from lfp_logging import logs
from openai import AsyncClient
from openinference.instrumentation.pydantic_ai import OpenInferenceSpanProcessor
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
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
    workspace_client: WorkspaceClient | None = None,
    **kwargs: Any,
) -> Agent:
    """Create a configured PydanticAI agent with baseline response instructions."""

    instructions = [_DEFAULT_INSTRUCTIONS]
    if kwargs_instructions := strs.trim(kwargs.pop("instructions", None)):
        for instruction in objects.to_list(kwargs_instructions, flatten=True):
            if instruction := strs.trim(instruction):
                instructions.append(instruction)
    kwargs["instructions"] = "\n\n".join(instructions)
    instrument = kwargs.get("instrument", None)
    if (instrument is None or instrument) and _configure_tracing():
        kwargs.setdefault("instrument", True)
    return Agent(
        model=model(model_name=model_name, workspace_client=workspace_client), **kwargs
    )


def client(workspace_client: WorkspaceClient | None = None) -> AsyncClient:
    return _client(workspace_client) if workspace_client else _client_default()


def _client(workspace_client: WorkspaceClient) -> AsyncClient:
    http_client = _http_client(workspace_client)
    client_params = {
        "base_url": workspace_client.config.host + "/serving-endpoints",
        "api_key": "no-token",  # Passing in a placeholder to pass validations, this will not be used
        "http_client": http_client,
    }
    return AsyncClient(**client_params)


@functools.cache
def _client_default() -> AsyncClient:
    return _client(clients.workspace_client())


@functools.cache
def large() -> Agent[None, str]:
    return create(models.large())


@functools.cache
def small() -> Agent[None, str]:
    return create(models.small())


def model(
    model_name: str | None = None, workspace_client: WorkspaceClient | None = None
) -> Model:
    if not model_name:
        model_name = models.large()
    provider = OpenAIProvider(openai_client=client(workspace_client))
    # noinspection PyTypeChecker
    return OpenAIChatModel(model_name=model_name, provider=provider)


def _http_client(workspace_client: WorkspaceClient) -> httpx.AsyncClient:
    class AsyncBearerAuth(httpx.Auth):
        def __init__(self, header_fn):
            self._header_fn = header_fn

        async def async_auth_flow(self, request: httpx.Request):
            # Databricks SDK authenticate() is sync, but safe to call
            auth_headers = self._header_fn()
            request.headers["Authorization"] = auth_headers["Authorization"]
            yield request

    # noinspection PyProtectedMember
    bearer_auth = AsyncBearerAuth(
        workspace_client.serving_endpoints._api._cfg.authenticate
    )
    try:
        import h2  # noqa: F401  # pyright: ignore[reportMissingImports]

        http2 = True
    except Exception:
        http2 = False
    return httpx.AsyncClient(
        auth=bearer_auth,
        http2=http2,
    )


@functools.cache
def _configure_tracing() -> bool:
    """Configure OpenTelemetry export to Phoenix collector."""
    if expiriment_id := os.environ.get("MLFLOW_EXPERIMENT_NAME", None):
        experiment = experiments.get(experiment_request=expiriment_id)
    elif expiriment_name := os.environ.get("MLFLOW_EXPERIMENT_NAME", None):
        experiment = experiments.get(experiment_request=expiriment_name)
    else:
        return False
    config = configs.get()
    exporter_endpoint = f"{config.host.rstrip('/')}/api/2.0/mlflow/otel/v1/traces"
    exporter_headers: dict[str, str] = {
        "Authorization": f"Bearer {configs.token(config)}",
        "x-mlflow-experiment-id": experiment.experiment_id,
    }
    tracer_provider = TracerProvider()
    exporter = OTLPSpanExporter(endpoint=exporter_endpoint, headers=exporter_headers)
    tracer_provider.add_span_processor(OpenInferenceSpanProcessor())
    tracer_provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(tracer_provider)
    LOG.info(
        f"Configured OpenTelemetry export to Phoenix collector: {exporter_endpoint}"
    )
    return True


if __name__ == "__main__":
    print(create())
