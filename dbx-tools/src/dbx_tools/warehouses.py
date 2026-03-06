import enum
import json
import time
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    EndpointInfo,
    ExecuteStatementRequestOnWaitTimeout,
    StatementResponse,
    StatementState,
)
from dbx_core import strs
from pydantic import BaseModel, Field, computed_field

from dbx_tools.catalogs import CatalogSchemaTable, CatalogSchemaTableLike
from dbx_tools.clients import _WAREHOUSE_SIZE_PATTERN
from dbx_tools.clients import workspace_client as default_workspace_client


class WarehouseSort(enum.Enum):
    """Supported sort dimensions for SQL warehouse ranking."""

    SERVERLESS = 1
    SIZE = 2
    NAME = 3


class TableType(str, enum.Enum):
    """
    Enumeration of Databricks table and view types.

    This enum represents the object type returned by Databricks SQL metadata
    such as `DESCRIBE TABLE EXTENDED` or `information_schema.tables.table_type`.
    """

    def __new__(cls, value: str, description: str):
        obj = str.__new__(cls, value)
        obj._value_ = value
        obj.description = description
        return obj

    MANAGED = (
        "MANAGED",
        "Managed Delta table (Databricks owns storage)",
    )
    EXTERNAL = (
        "EXTERNAL",
        "External table (storage location managed outside Databricks)",
    )
    VIEW = (
        "VIEW",
        "Standard SQL view",
    )
    MATERIALIZED_VIEW = (
        "MATERIALIZED_VIEW",
        "Materialized view",
    )
    METRIC_VIEW = (
        "METRIC_VIEW",
        "Databricks metric view",
    )
    STREAMING_TABLE = (
        "STREAMING_TABLE",
        "Table created via streaming / DLT",
    )
    TEMP_VIEW = (
        "TEMP_VIEW",
        "Temporary view",
    )
    GLOBAL_TEMP_VIEW = (
        "GLOBAL_TEMP_VIEW",
        "Global temporary view",
    )


from typing import Any

from pydantic import BaseModel, Field, computed_field


class TableDescription(BaseModel):
    """
    Representation of a Databricks table or view description.

    This model wraps metadata typically returned from commands like
    `DESCRIBE TABLE EXTENDED` and exposes normalized, LLM-friendly
    computed fields for commonly accessed properties such as table type
    and view definitions.
    """

    name: CatalogSchemaTable = Field(
        ...,
        description=(
            "Fully qualified table identifier including catalog, schema, "
            "and table name. This uniquely identifies the table or view "
            "within the Databricks Unity Catalog."
        ),
    )

    data: dict[str, Any] = Field(
        ...,
        description=(
            "Raw metadata dictionary returned from Databricks table "
            "inspection commands such as DESCRIBE TABLE EXTENDED. "
            "Keys may include fields like 'type', 'view_text', storage "
            "information, or other metadata properties."
        ),
        min_length=1,
    )

    @computed_field(
        description=(
            "The Databricks table type derived from the metadata 'type' field. "
            "This indicates whether the object is a managed table, external table, "
            "view, materialized view, metric view, streaming table, or temporary view."
        )
    )
    @property
    def table_type(self) -> TableType:
        table_type = self.data.get("type", None)
        if table_type:
            return TableType(table_type)
        raise RuntimeError(f"Table '{self.name}' has no type")

    @computed_field(
        description=(
            "SQL definition text for views or metric views if available. "
            "For regular tables this will be None."
        )
    )
    @property
    def view_text(self) -> str | None:
        view_text = strs.trim(self.data.get("view_text", None))
        return view_text or None

    @computed_field(
        description=(
            "Parsed metric view definition as a structured dictionary. "
            "Only populated when the table type is METRIC_VIEW and the "
            "view_text contains a YAML metric definition."
        )
    )
    @property
    def metric_view_definition(self) -> dict[str, Any] | None:
        if self.table_type == TableType.METRIC_VIEW:
            if view_text := self.view_text:
                return yaml.safe_load(view_text)
        return None


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


def execute_statement(
    statement: str,
    warehouse_id: str | None = None,
    workspace_client: WorkspaceClient | None = None,
    wait_timeout: str = "50s",
    poll_interval_seconds: float = 0.25,
    **kwargs,
) -> StatementResponse:
    """Execute SQL on a warehouse and poll until completion.

    Args:
        statement: SQL statement to execute.
        warehouse_id: Optional warehouse id. Top-ranked warehouse is used when omitted.
        workspace_client: Optional `WorkspaceClient`; default client is used when omitted.
        wait_timeout: Initial execution wait timeout passed to Databricks SQL API.
        poll_interval_seconds: Delay between polling calls while query is running.
        **kwargs: Additional keyword arguments forwarded to `execute_statement`.

    Returns:
        Final `StatementResponse` once the statement reaches `SUCCEEDED`.

    Raises:
        RuntimeError: If execution reaches a terminal non-success state.
    """
    wc = workspace_client or default_workspace_client()
    selected_warehouse_id = warehouse_id or get(workspace_client=wc).id
    response = wc.statement_execution.execute_statement(
        warehouse_id=selected_warehouse_id,
        statement=statement,
        wait_timeout=wait_timeout,
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
        **kwargs,
    )
    statement_id = response.statement_id
    status_response = response

    while True:
        status = getattr(status_response, "status", None)
        state = status.state if status else None
        if state == StatementState.SUCCEEDED:
            return status_response
        if state in {
            StatementState.FAILED,
            StatementState.CANCELED,
            StatementState.CLOSED,
        }:
            message = (
                status.error.message if status and status.error else "query failed"
            )
            raise RuntimeError(message)
        if not statement_id:
            raise RuntimeError("query did not return a statement id")
        time.sleep(max(poll_interval_seconds, 0))
        status_response = wc.statement_execution.get_statement(statement_id)


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


def describe_table_extended(
    name: CatalogSchemaTableLike,
    workspace_client: WorkspaceClient | None = None,
    warehouse_id: str | None = None,
    **kwargs: Any,
) -> TableDescription:
    """Return metric view YAML/SQL text from DESCRIBE TABLE EXTENDED AS JSON.

    Args:
        name: Metric view name accepted by ``CatalogSchemaTable.of``.
        workspace_client: Optional Databricks workspace client.
        warehouse_id: Optional SQL warehouse id. Uses ranked default when omitted.
        wait_timeout: Statement execution wait timeout.

    Returns:
        The ``view_text`` field from DESCRIBE TABLE EXTENDED JSON output.

    Raises:
        RuntimeError: When statement execution fails or ``view_text`` is missing.
    """
    fqname = CatalogSchemaTable.of(name)
    response = execute_statement(
        statement=f"DESCRIBE TABLE EXTENDED {fqname} AS JSON",
        warehouse_id=warehouse_id,
        workspace_client=workspace_client,
        **kwargs,
    )
    data_array = response.result.data_array if response.result else []
    if raw_json := data_array[0][0] if data_array and data_array[0] else None:
        try:
            payload = json.loads(raw_json)
        except json.JSONDecodeError:
            payload = None
        if payload:
            return TableDescription(name=fqname, data=payload)
    raise RuntimeError(
        f"Table'{fqname}' describe extended failed - response: {response}"
    )


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


if "__main__" == __name__:
    import os

    os.environ["DATABRICKS_CONFIG_PROFILE"] = "RACETRAC-DEV"
    dtable = describe_table_extended("databricks_demos.rtswv3.racetrac_kpi_metrics")
    print(dtable.model_dump_json())
    print(dtable.table_type)
    print(dtable.metric_view_definition)
    print(dtable.table_type)
