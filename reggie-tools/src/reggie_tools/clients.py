"""Client factory helpers shared across Databricks command-line tools."""

import functools
from datetime import datetime
from typing import Optional

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from pyspark.sql import SparkSession
from reggie_core import logs

from reggie_tools import configs, runtimes


def workspace_client(config: Config = None) -> WorkspaceClient:
    """Create a Databricks ``WorkspaceClient`` using the provided or cached config."""
    if not config:
        config = configs.get()
    return WorkspaceClient(config=config)


def spark(config: Optional[Config] = None) -> SparkSession:
    """Return a Spark session sourced from the runtime or Databricks Connect."""
    # Fast paths when no explicit config is provided
    if config is None:
        # Existing session injected in IPython user namespace
        sess = runtimes.ipython_user_ns("spark")
        if sess:
            return sess

        # Any active local Spark session
        sess = SparkSession.getActiveSession()
        if sess:
            return sess

        # Local Spark available via runtime
        if runtimes.version():
            return SparkSession.builder.getOrCreate()

        # Databricks Connect default session
        return _databricks_session_default()

    # Config provided or resolved above
    return DatabricksSession.builder.sdkConfig(config).getOrCreate()


@functools.cache
def _databricks_session_default() -> SparkSession:
    """Initialize and memoize the default Databricks Connect Spark session."""
    config = configs.get()
    log = logs.logger(__name__, __file__)
    log.info("databricks connect session initializing")
    start_time = datetime.now()
    sess = DatabricksSession.builder.sdkConfig(config).getOrCreate()
    elapsed = (datetime.now() - start_time).total_seconds()
    log.info(f"databricks connect session created in {elapsed:.2f}s")
    return sess


if __name__ == "__main__":
    pass
