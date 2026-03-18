"""Client factory helpers shared across Databricks command-line tools.

This module provides functions to create and manage Databricks ``WorkspaceClient`` and ``SparkSession`` objects.
It also includes logic for finding preferred SQL warehouses and retrieving project metadata for user-agent strings.
"""

import functools
import os
import re
from contextvars import ContextVar
from datetime import datetime
from typing import Callable

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from dbx_core import imports, projects
from lfp_logging import logs
from pyspark.sql import SparkSession

from dbx_tools import configs, runtimes

LOG = logs.logger(__name__)

_WAREHOUSE_SIZE_PATTERN = re.compile(r"((?:\d+x|x))?(.+)")
_HOST_CONTEXT: ContextVar[str | None] = ContextVar("databricks_host", default=None)
_TOKEN_CONTEXT: ContextVar[str | None] = ContextVar("databricks_token", default=None)
_FORCE_TOKEN_CONTEXT: ContextVar[bool] = ContextVar("force_token", default=False)


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
    1.  Uses `databricks-tools-core` logic if available.
    2.  Handles explicit host/token overrides (via ContextVars).
    3.  Manages OAuth M2M authentication for Databricks Apps (using client ID/secret).
    4.  Defaults to standard SDK authentication if no explicit configuration is provided.

    The client is configured with product identity in the user-agent.
    """
    if fn := _databricks_tools_core_get_workspace_client_fn():
        return fn()
    # Context variables allow request-scoped auth overrides in shared runtimes.
    host = _HOST_CONTEXT.get()
    token = _TOKEN_CONTEXT.get()
    force = _FORCE_TOKEN_CONTEXT.get()

    client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "")

    # Common kwargs for product identification in user-agent
    product_kwargs = {
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


def spark(connect: bool = True) -> SparkSession | None:
    """Return a Spark session sourced from runtime context or Databricks Connect.

    Args:
        connect: When ``True``, create a Databricks Connect session if no runtime
            Spark session is available. When ``False``, return ``None`` instead of
            creating a new connect-backed session.

    Returns:
        The active ``SparkSession`` when found or created, otherwise ``None``.
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
        from wrapt import LazyObjectProxy

        # noinspection PyTypeChecker
        return LazyObjectProxy(_load, interface=SparkSession)
    except ImportError:
        return _load()


@functools.cache
def _product_name() -> str:
    """Return the name of the root project."""
    return projects.root_project_name()


@functools.cache
def _product_version() -> str:
    """Return the version of the root project."""
    return projects.root_project_version()


@functools.cache
def _databricks_tools_core_get_workspace_client_fn() -> (
    Callable[[], WorkspaceClient] | None
):
    """Search for and return a `get_workspace_client` function from the `databricks_tools_core` package.

    This function attempts to use `databricks-tools-core` for workspace client creation
    if the module is available. This ensures that if the import is available, we'll
    use its established logic. However, since the import might not be available,
    equivalent logic is also implemented within this module's `_workspace_client`.
    """
    module_name = "databricks_tools_core"
    if imports.resolve_module(module_name, execute=False):
        import logging

        class IgnoreVersionWarning(logging.Filter):
            def filter(self, record):
                message = record.getMessage() if record else None
                if message and "VERSION file not found" in message:
                    return False
                else:
                    return True

        logger = logging.getLogger(f"{module_name}.identity")
        logger.addFilter(IgnoreVersionWarning())
        return imports.resolve(module_name, "get_workspace_client")
    return None
