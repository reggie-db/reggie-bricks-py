"""Client factory helpers shared across Databricks command-line tools.

This module provides functions to create and manage Databricks ``WorkspaceClient``,
``SparkSession`` and authenticated ``httpx.AsyncClient`` objects for direct REST
API calls. It also includes logic for finding preferred SQL warehouses and
retrieving project metadata for user-agent strings.
"""

import asyncio
import functools
import os
import signal
from contextvars import ContextVar
from datetime import datetime
from typing import Any, Literal, overload
from urllib.parse import urlparse

import httpx
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from dbx_core import projects
from lfp_logging import logs
from pyspark.sql import SparkSession

from dbx_tools import configs, runtimes

LOG = logs.logger(__name__)

_HOST_CONTEXT: ContextVar[str | None] = ContextVar("databricks_host", default=None)
_TOKEN_CONTEXT: ContextVar[str | None] = ContextVar("databricks_token", default=None)
_FORCE_TOKEN_CONTEXT: ContextVar[bool] = ContextVar("force_token", default=False)
_DEFAULT_API_TIMEOUT = httpx.Timeout(connect=10.0, read=300.0, write=30.0, pool=30.0)
_AUTHORIZATION_HEADER_NAME = "Authorization"


def workspace_client(config: Config | None = None) -> WorkspaceClient:
    """Create a Databricks ``WorkspaceClient`` using the provided or cached config.

    Args:
        config: Optional pre-built SDK config. When omitted, the cached default
            workspace client path is used.

    Returns:
        A configured ``WorkspaceClient`` instance.
    """
    if config:
        return WorkspaceClient(config=config)
    else:
        return _workspace_client_default()


@functools.cache
def _workspace_client_default() -> WorkspaceClient:
    """Create a Databricks ``WorkspaceClient`` with the current context and environment variables.

    This function implements a priority-based authentication flow for Databricks clients:
    1.  Handles explicit host/token overrides (via ContextVars).
    2.  Manages OAuth M2M authentication for Databricks Apps (using client ID/secret).
    3.  Defaults to standard SDK authentication when no explicit override is provided.

    The client is configured with product identity in the user-agent.
    """
    # Context variables allow request-scoped auth overrides in shared runtimes.
    host = _HOST_CONTEXT.get()
    token = _TOKEN_CONTEXT.get()
    force = _FORCE_TOKEN_CONTEXT.get()

    client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "")

    # Common kwargs for product identification in user-agent
    product_kwargs: dict[str, Any] = {
        "product": _product_name(),
        "product_version": _product_version(),
    }

    # Cross-workspace: explicit token overrides env OAuth so tool operations
    # target the caller-specified workspace instead of the app's own workspace
    if force and host and token:
        client = WorkspaceClient(host=host, token=token, **product_kwargs)
    elif bool(client_id and client_secret):
        # In Databricks Apps (OAuth credentials in env), explicitly use OAuth M2M
        # This prevents the SDK from detecting other auth methods like PAT or config file
        oauth_host = host or os.environ.get("DATABRICKS_HOST", "")

        # Explicitly configure OAuth M2M to prevent auth conflicts
        client = WorkspaceClient(
            host=oauth_host,
            client_id=client_id,
            client_secret=client_secret,
            **product_kwargs,
        )
    elif host and token:
        client = WorkspaceClient(host=host, token=token, **product_kwargs)
    elif host:
        client = WorkspaceClient(host=host, **product_kwargs)
    else:
        client = WorkspaceClient(profile=configs.profile(), **product_kwargs)
    client.config.with_user_agent_extra("project", _product_name())
    return client


@overload
def spark(connect: Literal[True] = True) -> SparkSession: ...


@overload
def spark(connect: bool) -> SparkSession | None: ...


def spark(connect: bool = True) -> SparkSession | None:
    """Return a Spark session sourced from runtime context or Databricks Connect.

    Args:
        connect: When ``True``, create a Databricks Connect session if no runtime
            Spark session is available. When ``False``, return ``None`` instead of
            creating a new connect-backed session.

    Returns:
        The active ``SparkSession`` when found or created, otherwise ``None``
        (only possible when ``connect=False``).
    """
    if instance := runtimes.ipython_user_ns("spark", None):
        return instance
    if runtimes.version():
        return SparkSession.builder.getOrCreate()
    return _spark() if connect else None


@functools.cache
def _spark() -> SparkSession:
    """Initialize a Spark session via Databricks Connect and cache the result.

    This function builds a session using the SDK configuration, measuring and
    logging initialization performance.
    """

    def _load():
        LOG.info("Databricks connect session initializing")
        start_time = datetime.now()
        sess = DatabricksSession.builder.sdkConfig(configs.get()).getOrCreate()
        elapsed = (datetime.now() - start_time).total_seconds()
        LOG.info(f"Databricks connect session created in {elapsed:.2f}s")
        return sess

    try:
        from wrapt import LazyObjectProxy  # pyright: ignore[reportAttributeAccessIssue]

        # noinspection PyTypeChecker
        return LazyObjectProxy(_load, interface=SparkSession)
    except ImportError:
        return _load()


def api(
    workspace_client: WorkspaceClient | None = None,
    timeout: httpx.Timeout | None = None,
) -> httpx.AsyncClient:
    """Return an authenticated ``httpx.AsyncClient`` for Databricks REST APIs.

    The returned client uses the workspace's host as its ``base_url`` so callers
    can write absolute REST paths like ``/api/2.0/genie/spaces/...``. A
    Databricks bearer token is injected on every outbound request via the SDK's
    authenticate header factory, re-resolving per call so OAuth refresh and
    rotation are handled transparently. HTTP/2 is enabled when the optional
    ``h2`` package is importable.
    """
    if workspace_client is None and timeout is None:
        return _api_default()
    return _api(workspace_client or _workspace_client_default(), timeout)


def _api(
    workspace_client: WorkspaceClient,
    timeout: httpx.Timeout | None = None,
) -> httpx.AsyncClient:
    """Return an authenticated ``httpx.AsyncClient`` for Databricks REST APIs.

    The returned client uses the workspace's host as its ``base_url`` so callers
    can write absolute REST paths like ``/api/2.0/genie/spaces/...``. A
    Databricks bearer token is injected on every outbound request via the SDK's
    authenticate header factory, re-resolving per call so OAuth refresh and
    rotation are handled transparently. HTTP/2 is enabled when the optional
    ``h2`` package is importable.

    The caller owns the client's lifecycle and should close it (preferably via
    ``async with``) to release the underlying connection pool.

    Args:
        workspace_client: Optional Databricks ``WorkspaceClient``. When omitted,
            the cached default workspace client (and therefore its host and
            credentials) is used.
        timeout: Optional ``httpx.Timeout`` override. Defaults to a long-read
            timeout suitable for Databricks polling endpoints.

    Returns:
        An authenticated ``httpx.AsyncClient`` ready to call Databricks REST APIs.
    """
    # ``workspace_client()`` with no config routes through the cached default; call
    # the cache directly here to avoid the parameter shadowing the function name.
    config = workspace_client.config if workspace_client else configs.get()
    return httpx.AsyncClient(
        base_url=config.host,
        auth=_AsyncBearerAuth(config),
        timeout=timeout if timeout is not None else _DEFAULT_API_TIMEOUT,
        http2=_http2_supported(),
    )


@functools.cache
def _api_default() -> httpx.AsyncClient:
    """Return the cached default authenticated httpx.AsyncClient."""
    api_client = _api(workspace_client=_workspace_client_default(), timeout=None)

    aclose_orig = api_client.aclose

    def _handle_exit(sig, frame):
        LOG.debug("Shutting down api client")
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Schedule the real closure on the running loop
                loop.create_task(aclose_orig())
            else:
                # Loop is idle/stopped, run it to completion
                loop.run_until_complete(aclose_orig())
        except Exception as e:
            LOG.warning("Error during api client shutdown", exc_info=True)

        raise SystemExit

    signal.signal(signal.SIGINT, _handle_exit)
    signal.signal(signal.SIGTERM, _handle_exit)

    async def aclose_noop():
        pass

    api_client.aclose = aclose_noop

    return api_client


@functools.cache
def _product_name() -> str:
    """Return the name of the root project."""
    return projects.root_project_name()


@functools.cache
def _product_version() -> str:
    """Return the version of the root project."""
    return projects.root_project_version()


@functools.cache
def _http2_supported() -> bool:
    """Return ``True`` when the optional ``h2`` package is installed."""
    try:
        import h2  # noqa: F401  # pyright: ignore[reportMissingImports]

        return True
    except ImportError:
        return False


class _AsyncBearerAuth(httpx.Auth):
    """Inject Databricks bearer auth headers on every outbound httpx request.

    The Databricks SDK's authenticate function is synchronous but cheap to call
    and returns the headers needed for each request. Calling it per-request
    ensures OAuth token rotation is observed without manual refresh handling.
    """

    def __init__(self, config: Config):
        if config_hostname := urlparse(config.host).hostname:
            self._hostname = config_hostname.lower()
        else:
            self._hostname = None
        self._token_fn = lambda: configs.token(config)
        # org_id may not always be present
        self._org_id = getattr(config, "account_id", None) or getattr(
            config, "org_id", None
        )

    async def async_auth_flow(self, request: httpx.Request):
        if self._hostname:
            if request_hostname := request.url.host:
                request_hostname = request_hostname.lower()
                if request_hostname == self._hostname or request_hostname.endswith(
                    "." + self._hostname
                ):
                    if token := self._token_fn():
                        request.headers[_AUTHORIZATION_HEADER_NAME] = "Bearer " + token

        yield request
