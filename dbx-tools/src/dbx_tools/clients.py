"""Client factory helpers shared across Databricks command-line tools."""

import functools
from datetime import datetime

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from lfp_logging import logs
from pyspark.sql import SparkSession

from dbx_tools import configs, runtimes

LOG = logs.logger(__name__)


def workspace_client() -> WorkspaceClient:
    """Create a Databricks ``WorkspaceClient`` using the provided or cached config.
    Uses the default cached config when none is supplied.
    """
    return WorkspaceClient(config=configs.get())


def spark() -> SparkSession:
    """Return a Spark session sourced from the runtime or Databricks Connect."""
    if instance := runtimes.ipython_user_ns("spark", None):
        return instance
    if runtimes.version():
        return SparkSession.builder.getOrCreate()
    return _spark()


@functools.cache
def _spark() -> SparkSession:
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
