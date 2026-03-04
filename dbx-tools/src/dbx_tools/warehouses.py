import enum

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import EndpointInfo
from dbx_core import strs

from dbx_tools.clients import _WAREHOUSE_SIZE_PATTERN
from dbx_tools.clients import workspace_client as default_workspace_client


class WarehouseSort(enum.Enum):
    """Supported sort dimensions for SQL warehouse ranking."""

    SERVERLESS = 1
    SIZE = 2
    NAME = 3


def get(
    *args,
    **kwargs,
) -> EndpointInfo:
    """Return the highest ranked accessible warehouse.

    Args:
        *args: Positional arguments forwarded to :func:`list`.
        **kwargs: Keyword arguments forwarded to :func:`list`.

    Returns:
        The top-ranked warehouse endpoint.

    Raises:
        ValueError: If there are no accessible warehouses with IDs.
    """
    ranked_warehouses = list(*args, **kwargs)
    if not ranked_warehouses:
        raise ValueError("No accessible SQL warehouses found")
    return ranked_warehouses[0]


def list(
    *sort: WarehouseSort,
    name_preference: list[str] | None = None,
    workspace_client: WorkspaceClient | None = None,
) -> list[EndpointInfo]:
    """Return accessible warehouses sorted by requested ranking dimensions.

    Args:
        *sort: One or more `WarehouseSort` values defining ranking order.
        name_preference: Ordered list of preferred name fragments.
        workspace_client: Optional `WorkspaceClient`; default client is used when omitted.

    Returns:
        Warehouses sorted by the selected ranking dimensions, highest ranked first.
    """
    if not sort:
        sort = tuple(WarehouseSort)
    if name_preference is None:
        name_preference = ["shared", "tools"]
    normalized_name_preference = [
        preference.lower() for preference in name_preference if preference
    ]

    wc = workspace_client or default_workspace_client()
    candidates = [w for w in wc.warehouses.list() if w.id]
    if not candidates:
        return []

    name_tokens_by_id: dict[str, list[str]] = {}
    for candidate in candidates:
        name_tokens_by_id[candidate.id] = [*strs.tokenize(candidate.name)]

    def _name_rank(w: EndpointInfo) -> int:
        name_tokens = name_tokens_by_id.get(w.id, [])
        if not normalized_name_preference:
            return 0
        for idx, preference in enumerate(normalized_name_preference):
            if preference in name_tokens:
                return len(normalized_name_preference) - idx
        return 0

    def _key(w: EndpointInfo) -> tuple[int, ...]:
        rank: list[int] = []
        for current_sort in sort:
            if current_sort is WarehouseSort.SERVERLESS:
                rank.append(1 if bool(w.enable_serverless_compute) else 0)
            elif current_sort is WarehouseSort.SIZE:
                rank.append(_warehouse_size_rank(w.cluster_size))
            elif current_sort is WarehouseSort.NAME:
                rank.append(_name_rank(w))
        return tuple(rank)

    return sorted(candidates, key=_key, reverse=True)


def _warehouse_size_rank(cluster_size: str | None) -> int:
    """Rank warehouses by cluster size (larger => higher rank)."""

    cluster_size_normalized = "".join(strs.tokenize(cluster_size))

    if not cluster_size_normalized:
        return 0
    if m := _WAREHOUSE_SIZE_PATTERN.match(cluster_size_normalized):
        if name := m.group(2):
            sizing = {"small": -1, "large": 1}
            if name in sizing:
                rank = sizing[name]
                multiplier_value = m.group(1)
                if multiplier_value:
                    multiplier = (
                        int(multiplier_value[:-1]) if len(multiplier_value) > 1 else 1
                    )
                    rank = rank * (multiplier + 1)
                return rank
    return 0
