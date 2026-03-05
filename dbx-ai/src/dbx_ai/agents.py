import functools

import httpx
from databricks.sdk import WorkspaceClient
from dbx_tools import clients
from openai import AsyncClient
from pydantic_ai import Agent
from pydantic_ai.models import Model
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider

from dbx_ai import models


def create(
    model_name: str | None = None, workspace_client: WorkspaceClient | None = None
) -> Agent:
    return Agent(model(model_name=model_name, workspace_client=workspace_client))


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
    if model_name is None:
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


if __name__ == "__main__":
    print(create())
