import asyncio
import enum
import json
import re
import time
from builtins import list as py_list
from typing import Any

import httpx
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

from dbx_tools import clients
from dbx_tools.catalogs import CatalogSchemaTable, CatalogSchemaTableLike

_DEFAULT_WAIT_TIMEOUT_SECONDS = 50
_DEFAULT_POLL_INTERVAL_SECONDS = 0.1
_WAREHOUSE_SIZE_PATTERN = re.compile(r"((?:\d+x|x))?(.+)")


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

    # Class-level annotation (no value) so Pyright knows every member has a
    # ``description`` attribute. Python's enum machinery ignores annotation-only
    # entries, so this does NOT become an enum member.
    description: str

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


class StatementExecutionError(Exception):
    def __init__(
        self,
        message: str | None = None,
        statement_id: str | None = None,
        statement_response: StatementResponse | None = None,
    ):
        self.message = message or ""

        statement_response_data = None
        if self.statement_response:
            try:
                statement_response_data = self.statement_response.as_dict()
            except Exception:
                pass
        exception_message = f"Statement failed (statement_id={self.statement_id} statement_response={statement_response_data})"
        if self.message:
            exception_message += f": {self.message}"
        super().__init__(exception_message)


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


def _get_id(*args, **kwargs) -> str:
    warehouse_id = get(*args, **kwargs).id
    if not warehouse_id:
        raise ValueError("No warehouse ID selected")
    return warehouse_id


def execute_statement(
    statement: str,
    warehouse_id: str | None = None,
    workspace_client: WorkspaceClient | None = None,
    wait_timeout: str | float = _DEFAULT_POLL_INTERVAL_SECONDS,
    poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL_SECONDS,
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
        StatementExecutionError: If execution reaches a terminal non-success state.
        ValueError: When no warehouse can be selected.
    """
    wc = workspace_client or clients.workspace_client()
    selected_warehouse_id = warehouse_id or _get_id(workspace_client=wc)
    statement_resp = wc.statement_execution.execute_statement(
        warehouse_id=selected_warehouse_id,
        statement=statement,
        wait_timeout=_wait_timeout(wait_timeout),
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
        **kwargs,
    )
    if _statement_response_terminal(statement_resp):
        return statement_resp
    return _statement_response_wait(
        statement_resp.statement_id,
        workspace_client=wc,
        poll_interval_seconds=poll_interval_seconds,
    )


def statement_response(
    statement_id: str,
    workspace_client: WorkspaceClient | None = None,
    poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL_SECONDS,
) -> StatementResponse:
    wc = workspace_client or clients.workspace_client()
    return _statement_response_wait(
        statement_id, workspace_client=wc, poll_interval_seconds=poll_interval_seconds
    )


def _statement_response_wait(
    statement_id: str | None,
    workspace_client: WorkspaceClient,
    poll_interval_seconds: float,
) -> StatementResponse:
    while True:
        if statement_id:
            statement_resp = workspace_client.statement_execution.get_statement(
                statement_id=statement_id
            )
            if _statement_response_terminal(statement_resp):
                return statement_resp
            statement_id = statement_resp.statement_id
            time.sleep(max(poll_interval_seconds, 0))
        else:
            raise StatementExecutionError("statement_id not found")


async def execute_statement_async(
    statement: str,
    warehouse_id: str | None = None,
    workspace_client: WorkspaceClient | None = None,
    wait_timeout: str | float | int = _DEFAULT_WAIT_TIMEOUT_SECONDS,
    poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL_SECONDS,
    **kwargs: Any,
) -> StatementResponse:
    """Async equivalent of :func:`execute_statement`.

    Talks to the SQL Statement Execution REST API directly via
    :func:`dbx_tools.clients.api` (mirroring the pattern used by
    :class:`dbx_tools.genie.GenieConversation`) so the call slots cleanly into
    an ``asyncio`` event loop without occupying a thread for the duration of a
    long-running query.

    Same return type and error semantics as the sync version:

    * Returns :class:`StatementResponse` once the statement reaches
      :attr:`StatementState.SUCCEEDED`.
    * Raises :class:`StatementExecutionError` when the statement enters
      ``FAILED``, ``CANCELED``, or ``CLOSED``.
    * Raises ``ValueError`` when no warehouse can be selected.
    * Non-2xx HTTP responses propagate as :class:`httpx.HTTPStatusError`.

    Args:
        statement: SQL statement to execute.
        warehouse_id: Optional warehouse id. The top-ranked warehouse from
            :func:`get` is used when omitted.
        api_client: Optional pre-built authenticated ``httpx.AsyncClient``.
            When omitted, the cached default client returned by
            :func:`dbx_tools.clients.api` is used.
        workspace_client: Optional ``WorkspaceClient`` consulted only when
            ``warehouse_id`` is omitted (used by :func:`get` to resolve the
            ranked default).
        wait_timeout: Initial execution wait timeout passed to the SQL API.
        poll_interval_seconds: Delay between status polls while the query is
            still running.
        **kwargs: Extra body fields forwarded verbatim to the
            ``POST /api/2.0/sql/statements`` request (eg. ``catalog``,
            ``schema``, ``parameters``, ``row_limit``). Callers are
            responsible for serialising any enum values to their ``.value``
            form ahead of time.

    Returns:
        Final :class:`StatementResponse` once the statement reaches
        ``SUCCEEDED``.
    """
    selected_warehouse_id = warehouse_id or _get_id(workspace_client=workspace_client)
    api_client = clients.api(workspace_client=workspace_client)

    # Mirror the SDK's body shape; ``on_wait_timeout`` is enum-serialised so
    # callers don't need to know the API string spelling.
    body: dict[str, Any] = {
        "warehouse_id": selected_warehouse_id,
        "statement": statement,
        "wait_timeout": _wait_timeout(wait_timeout),
        "on_wait_timeout": ExecuteStatementRequestOnWaitTimeout.CONTINUE.value,
        **kwargs,
    }

    exec_resp = await api_client.post("/api/2.0/sql/statements", json=body)
    exec_resp.raise_for_status()
    statement_resp = StatementResponse.from_dict(exec_resp.json())
    if _statement_response_terminal(statement_resp):
        return statement_resp
    return await _statement_response_wait_async(
        statement_resp.statement_id, api_client, poll_interval_seconds
    )


async def statement_response_async(
    statement_id: str,
    client: WorkspaceClient | httpx.AsyncClient | None = None,
    poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL_SECONDS,
) -> StatementResponse:
    if isinstance(client, WorkspaceClient):
        api_client = clients.api(workspace_client=client)
    elif client is None:
        api_client = clients.api()
    else:
        api_client = client
    return await _statement_response_wait_async(
        statement_id, api_client=api_client, poll_interval_seconds=poll_interval_seconds
    )


async def _statement_response_wait_async(
    statement_id: str | None,
    api_client: httpx.AsyncClient,
    poll_interval_seconds: float,
) -> StatementResponse:
    while True:
        if statement_id:
            poll_resp = await api_client.get(f"/api/2.0/sql/statements/{statement_id}")
            poll_resp.raise_for_status()
            statement_resp = StatementResponse.from_dict(poll_resp.json())
            if _statement_response_terminal(statement_resp):
                return statement_resp
            statement_id = statement_resp.statement_id
            await asyncio.sleep(max(poll_interval_seconds, 0))
        else:
            raise StatementExecutionError("statement_id not found")


def _statement_response_terminal(statement_resp: StatementResponse | None) -> bool:
    if statement_resp:
        if status := statement_resp.status:
            if state := status.state:
                if state == StatementState.SUCCEEDED:
                    return True
                elif statement_resp.statement_id and state in {
                    StatementState.PENDING,
                    StatementState.RUNNING,
                }:
                    return False
    raise StatementExecutionError(
        "Invalid statement response", statement_response=statement_resp
    )


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

    wc = workspace_client or clients.workspace_client()
    candidates = [w for w in wc.warehouses.list() if w.id]
    if not candidates:
        return []

    name_tokens_by_id: dict[str, py_list[str]] = {}
    for candidate in candidates:
        if not candidate.id:
            continue
        name_tokens_by_id[candidate.id] = [*strs.tokenize(candidate.name)]

    def _name_rank(w: EndpointInfo) -> int:
        if w.id:
            name_tokens = name_tokens_by_id.get(w.id, [])
            if normalized_name_preference:
                for idx, preference in enumerate(normalized_name_preference):
                    if preference in name_tokens:
                        return len(normalized_name_preference) - idx
        return 0

    def _key(w: EndpointInfo) -> tuple[int, ...]:
        rank: py_list[int] = []
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
    """Return parsed metadata from ``DESCRIBE TABLE EXTENDED ... AS JSON``.

    Args:
        name: Table or view name accepted by ``CatalogSchemaTable.of``.
        workspace_client: Optional Databricks workspace client.
        warehouse_id: Optional SQL warehouse id. Uses ranked default when omitted.
        **kwargs: Additional statement execution options forwarded to
            ``execute_statement`` (for example ``wait_timeout``).

    Returns:
        Parsed table metadata wrapped in ``TableDescription``.

    Raises:
        RuntimeError: When statement execution fails or JSON output is missing.
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
        f"Table '{fqname}' describe extended failed - response: {response}"
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


def _wait_timeout(value: str | float | int) -> str:
    if isinstance(value, str):
        return value
    return f"{int(value)}s"


if "__main__" == __name__:
    import os

    os.environ["DATABRICKS_CONFIG_PROFILE"] = "RACETRAC-DEV"
    dtable = describe_table_extended("databricks_demos.rtswv3.racetrac_kpi_metrics")
    # print(dtable.model_dump_json())
    print(json.dumps(dtable.metric_view_definition, indent=2))
