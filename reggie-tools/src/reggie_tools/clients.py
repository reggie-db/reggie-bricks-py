"""Client factory helpers shared across Databricks command-line tools."""

import functools
from datetime import datetime

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from lfp_logging import logs
from pyspark.sql import SparkSession

from reggie_tools import configs, runtimes


def workspace_client(config: Config | None = None) -> WorkspaceClient:
    """Create a Databricks ``WorkspaceClient`` using the provided or cached config.
    Uses the default cached config when none is supplied.
    """
    if not config:
        config = configs.get()
    return WorkspaceClient(config=config)


def spark(config: Config | None = None) -> SparkSession:
    """Return a Spark session sourced from the runtime or Databricks Connect."""
    # Fast path when no explicit config is provided
    if config is None:
        # Prefer an existing session injected in the IPython user namespace
        sess = runtimes.ipython_user_ns("spark")
        if sess:
            return sess

        # Fallback to any active local Spark session
        sess = SparkSession.getActiveSession()
        if sess:
            return sess

        # Use a local Spark session when running inside a Databricks runtime
        if runtimes.version():
            return SparkSession.builder.getOrCreate()

        # Create a Databricks Connect session using default settings
        return _databricks_session_default()

    # Config provided or resolved above
    return DatabricksSession.builder.sdkConfig(config).getOrCreate()


@functools.cache
def _databricks_session_default() -> SparkSession:
    """Initialize and memoize the default Databricks Connect Spark session."""
    config = configs.get()
    log = logs.logger()
    log.info("Databricks connect session initializing")
    start_time = datetime.now()
    sess = DatabricksSession.builder.sdkConfig(config).getOrCreate()
    elapsed = (datetime.now() - start_time).total_seconds()
    log.info(f"Databricks connect session created in {elapsed:.2f}s")
    return sess
