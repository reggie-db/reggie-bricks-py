"""Client factory helpers shared across Databricks command-line tools."""

from datetime import datetime

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from lfp_logging import logs
from pyspark.sql import SparkSession

from dbx_tools import configs, platform, runtimes

LOG = logs.logger(__name__)


def workspace_client(config: Config | None = None) -> WorkspaceClient:
    """Create a Databricks ``WorkspaceClient`` using the provided or cached config.
    Uses the default cached config when none is supplied.
    """
    if not config:
        config = configs.get()
    return WorkspaceClient(config=config)


def spark(config: Config | None = None) -> SparkSession:
    """Return a Spark session sourced from the runtime or Databricks Connect."""

    if config is None:

        def _load():
            if runtimes.version():
                return SparkSession.builder.getOrCreate()
            else:
                default_config = configs.get()
                LOG.info("Databricks connect session initializing")
                start_time = datetime.now()
                sess = DatabricksSession.builder.sdkConfig(default_config).getOrCreate()
                elapsed = (datetime.now() - start_time).total_seconds()
                LOG.info(f"Databricks connect session created in {elapsed:.2f}s")
                return sess

        return platform.instance("spark", _load)

    return DatabricksSession.builder.sdkConfig(config).getOrCreate()
