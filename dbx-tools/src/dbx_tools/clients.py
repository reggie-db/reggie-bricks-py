"""Client factory helpers shared across Databricks command-line tools.

This module provides functions to create and manage Databricks ``WorkspaceClient`` and ``SparkSession`` objects.
It also includes logic for finding preferred SQL warehouses and retrieving project metadata for user-agent strings.
"""

import functools
import os
import re
from contextvars import ContextVar
from datetime import datetime

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from dbx_core import projects
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
