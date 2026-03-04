import functools

import httpx
from databricks.sdk import WorkspaceClient
from dbx_tools import clients
from openai import AsyncClient
from pydantic_ai import Agent
from pydantic_ai.models import Model
from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.providers.openai import OpenAIProvider

from dbx_ai import models


def create(model_name: str | None = None, wc: WorkspaceClient | None = None) -> Agent:
    return Agent(model(model_name=model_name, wc=wc))


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


def model(model_name: str | None = None, wc: WorkspaceClient | None = None) -> Model:
    if model_name is None:
        model_name = models.reasoning()
    provider = OpenAIProvider(openai_client=client(wc))
    # noinspection PyTypeChecker
    return OpenAIModel(model_name=model_name, provider=provider)


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
        import h2  # noqa: F401

        http2 = True
    except Exception:
        http2 = False
    return httpx.AsyncClient(
        auth=bearer_auth,
        http2=http2,
    )


if __name__ == "__main__":
    print(create())
