import functools
from dataclasses import dataclass

import httpx
from databricks.sdk import WorkspaceClient
from dbx_tools import clients
from openai import AsyncClient
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIModel


def agent(
    model: str = "databricks-gpt-5-2", wc: WorkspaceClient | None = None
) -> Agent:
    @dataclass
    class _OpenAIModelProvider:
        client: AsyncClient

    # noinspection PyTypeChecker
    model = OpenAIModel(model_name=model, provider=_OpenAIModelProvider(client(wc)))
    return Agent(model)


def client(wc: WorkspaceClient | None = None) -> AsyncClient:
    return _client(wc) if wc else _client_default()


def _client(wc: WorkspaceClient) -> AsyncClient:
    http_client = _http_client(wc)
    client_params = {
        "base_url": wc.config.host + "/serving-endpoints",
        "api_key": "no-token",  # Passing in a placeholder to pass validations, this will not be used
        "http_client": http_client,
    }
    return AsyncClient(**client_params)


@functools.cache
def _client_default() -> AsyncClient:
    return _client(clients.workspace_client())


def _http_client(wc: WorkspaceClient) -> httpx.AsyncClient:
    class AsyncBearerAuth(httpx.Auth):
        def __init__(self, header_fn):
            self._header_fn = header_fn

        async def async_auth_flow(self, request: httpx.Request):
            # Databricks SDK authenticate() is sync, but safe to call
            auth_headers = self._header_fn()
            request.headers["Authorization"] = auth_headers["Authorization"]
            yield request

    # noinspection PyProtectedMember
    bearer_auth = AsyncBearerAuth(wc.serving_endpoints._api._cfg.authenticate)
    try:
        http2 = True
    except Exception:
        http2 = False
    return httpx.AsyncClient(
        auth=bearer_auth,
        http2=http2,
    )
