"""Client factory helpers shared across Databricks command-line tools."""

import functools
from datetime import datetime
from typing import Any, Iterable

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from databricks_tools_core import get_workspace_client
from lfp_logging import logs
from pyspark.sql import SparkSession

from dbx_tools import configs, runtimes

LOG = logs.logger(__name__)


def workspace_client(config: Config | None = None) -> WorkspaceClient:
    """Create a Databricks ``WorkspaceClient`` using the provided or cached config.
    Uses the default cached config when none is supplied.
    """
    if config is None:
        return _workspace_client_default()
    return WorkspaceClient(config=configs.get())


@functools.cache
def _workspace_client_default() -> WorkspaceClient:
    """Create a Databricks ``WorkspaceClient`` using the default cached config."""
    return get_workspace_client()


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


def warehouse(client: WorkspaceClient | None = None) -> Any:
    """Return the preferred SQL warehouse we have access to.

    Selection priority (highest to lowest):
    - Prefer warehouses with serverless compute enabled.
    - Then prefer the largest warehouse size.
    - Then prefer names containing "shared", followed by "demo".

    Args:
        client: Optional `WorkspaceClient` to use. When omitted, uses the default
            client from `workspace_client()`.

    Returns:
        A warehouse record from the Databricks SDK (type depends on SDK version).

    Raises:
        ValueError: If no warehouses are found or none have an id.
    """

    wc = client or workspace_client()
    warehouses: Iterable[Any] = wc.warehouses.list()

    candidates: list[Any] = []
    for w in warehouses:
        if getattr(w, "id", None):
            candidates.append(w)

    if not candidates:
        raise ValueError("No accessible SQL warehouses found")

    def _key(w: Any) -> tuple[int, int, int]:
        serverless = 1 if bool(getattr(w, "enable_serverless_compute", False)) else 0
        size_rank = _warehouse_size_rank(getattr(w, "cluster_size", None))
        name_rank = _warehouse_name_rank(getattr(w, "name", None))
        return serverless, size_rank, name_rank

    return max(candidates, key=_key)


def _warehouse_name_rank(name: str | None) -> int:
    """Rank warehouses by name preference."""

    if not name:
        return 0
    n = name.lower()
    if "shared" in n:
        return 2
    if "demo" in n:
        return 1
    return 0


def _warehouse_size_rank(cluster_size: str | None) -> int:
    """Rank warehouses by cluster size (larger => higher rank)."""

    if not cluster_size:
        return 0

    # Observed values include: "2X-Small", "X-Large", "Small".
    normalized = str(cluster_size).strip().lower().replace(" ", "").replace("_", "")
    normalized = normalized.replace("x-", "x").replace("-", "")

    ranks = {
        "2xsmall": 10,
        "xsmall": 20,
        "small": 30,
        "medium": 40,
        "large": 50,
        "xlarge": 60,
        "2xlarge": 70,
        "3xlarge": 80,
        "4xlarge": 90,
    }
    return ranks.get(normalized, 0)


if __name__ == "__main__":
    w = warehouse()
    print(w.id)
    print(w.name)
    print(w)
