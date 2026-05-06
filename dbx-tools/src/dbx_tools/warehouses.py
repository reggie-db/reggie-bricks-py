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
    """Raised when a Databricks SQL statement fails or its response is unusable.

    Surfaced from :func:`execute_statement`, :func:`execute_statement_async`,
    :func:`statement_response`, :func:`statement_response_async`, and the
    private polling helpers (via :func:`_statement_response_terminal`). Carries
    the optional human-readable ``message`` and the last observed
    :class:`StatementResponse` (when available) so callers can inspect
    ``status.error`` / ``state`` for granular failure detail rather than
    parsing the formatted exception string.

    Attributes:
        message: Caller-supplied summary; empty string when omitted.
        statement_response: Last observed :class:`StatementResponse`, or
            ``None`` when not available (e.g. raised before any response was
            received).
    """

    def __init__(
        self,
        message: str | None = None,
        statement_response: StatementResponse | None = None,
    ):
        self.message = message or ""
        self.statement_response = statement_response

        statement_response_data = None
        if self.statement_response:
            try:
                statement_response_data = self.statement_response.as_dict()
            except Exception:
                pass
        exception_message = (
            f"Statement failed (statement_response={statement_response_data})"
        )
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
    wait_timeout: str | float = _DEFAULT_WAIT_TIMEOUT_SECONDS,
    poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL_SECONDS,
    **kwargs,
) -> StatementResponse:
    """Execute SQL on a warehouse and poll until completion.

    Args:
        statement: SQL statement to execute.
        warehouse_id: Optional warehouse id. Top-ranked warehouse (via
            :func:`get`) is used when omitted.
        workspace_client: Optional `WorkspaceClient`; the cached default
            client is used when omitted.
        wait_timeout: Initial execution wait timeout passed to the Databricks
            SQL API. Numeric values (``int`` / ``float``) are coerced to
            ``"<int>s"``; pass a string to control the unit yourself.
        poll_interval_seconds: Delay between polling calls while the query is
            still running.
        **kwargs: Additional keyword arguments forwarded verbatim to the SDK's
            ``WorkspaceClient.statement_execution.execute_statement`` (eg.
            ``catalog``, ``schema``, ``parameters``, ``row_limit``,
            ``disposition``).

    Returns:
        Final `StatementResponse` once the statement reaches `SUCCEEDED`.

    Raises:
        StatementExecutionError: When the statement reaches ``FAILED``,
            ``CANCELED``, or ``CLOSED``; when the response shape is
            unrecognised (no ``status.state``); or when polling cannot
            continue because the response is missing a ``statement_id``.
        ValueError: When no warehouse can be selected (none accessible, or
            the top-ranked one has no id).
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
    """Block until an already-submitted statement reaches a terminal state.

    Use when the statement was started elsewhere (e.g. via the Databricks UI
    or the SDK directly) and you only have the ``statement_id``. Internally
    polls ``statement_execution.get_statement`` on the workspace SDK client.

    Args:
        statement_id: Existing Databricks SQL statement id to wait on.
        workspace_client: Optional ``WorkspaceClient``. When omitted, the
            cached default client is used.
        poll_interval_seconds: Delay between status polls while the query is
            still running.

    Returns:
        Final :class:`StatementResponse` once the statement reaches
        ``SUCCEEDED``.

    Raises:
        StatementExecutionError: Same conditions as :func:`execute_statement`
            (terminal failure state, unrecognised response shape, or missing
            ``statement_id`` mid-poll).
    """
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
        workspace_client: Optional ``WorkspaceClient`` used both to resolve
            the ranked default warehouse (when ``warehouse_id`` is omitted)
            and to mint the authenticated httpx client via
            :func:`dbx_tools.clients.api`. When omitted, the cached defaults
            from those helpers are used.
        wait_timeout: Initial execution wait timeout passed to the SQL API.
            Numeric values (``int`` / ``float``) are coerced to ``"<int>s"``;
            pass a string to control the unit yourself.
        poll_interval_seconds: Delay between status polls while the query is
            still running.
        **kwargs: Extra body fields forwarded verbatim to the
            ``POST /api/2.0/sql/statements`` request (eg. ``catalog``,
            ``schema``, ``parameters``, ``row_limit``). Callers are
            responsible for serialising any enum values to their ``.value``
            form ahead of time (this function does NOT do the SDK-style
            kwarg-to-body translation that :func:`execute_statement` gets
            for free).

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
    """Async equivalent of :func:`statement_response`.

    Polls the SQL Statement Execution REST API directly via
    :func:`dbx_tools.clients.api` until ``statement_id`` reaches a terminal
    state, without occupying a thread for the duration of a long-running
    query.

    Args:
        statement_id: Existing Databricks SQL statement id to wait on.
        client: Either a :class:`WorkspaceClient` (used to mint the
            authenticated httpx client), an already-built
            :class:`httpx.AsyncClient` (used directly), or ``None`` to fall
            back to :func:`dbx_tools.clients.api`'s cached default.
        poll_interval_seconds: Delay between status polls while the query is
            still running.

    Returns:
        Final :class:`StatementResponse` once the statement reaches
        ``SUCCEEDED``.

    Raises:
        StatementExecutionError: When the statement reaches a terminal
            failure state, the response shape is unrecognised, or polling
            cannot continue because the response is missing a
            ``statement_id``.
        httpx.HTTPStatusError: For non-2xx HTTP responses from the SQL API.
    """
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
    """Tell the polling loop whether to return, keep polling, or fail.

    Tri-state behaviour packed into a ``bool`` return + raise:

    * Returns ``True`` when ``state == SUCCEEDED`` (caller should return the
      ``statement_resp`` as the final result).
    * Returns ``False`` when the statement is still in progress (``PENDING``
      / ``RUNNING``) **and** the response carries a ``statement_id`` for the
      caller to keep polling against.
    * **Raises** :class:`StatementExecutionError` for everything else: any
      terminal failure state (``FAILED`` / ``CANCELED`` / ``CLOSED``), an
      unparseable response (no ``status`` / no ``state``), or an in-progress
      response with no ``statement_id`` to poll against next.
    """
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
        RuntimeError: When the response is missing or the JSON payload cannot
            be parsed into a non-empty :class:`TableDescription`.
        StatementExecutionError: Propagated from :func:`execute_statement`
            when the underlying ``DESCRIBE TABLE EXTENDED`` query reaches a
            terminal failure state.
        ValueError: Propagated from :func:`execute_statement` when no
            warehouse can be selected.
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
    """Coerce a timeout to the ``"<seconds>s"`` string the SQL API expects.

    String inputs pass through verbatim (so callers can specify other units,
    e.g. ``"500ms"``, when supported). Numeric inputs are floor-truncated to
    whole seconds via ``int()``: passing ``0.5`` yields ``"0s"`` (a no-wait),
    not ``"500ms"``. Use a string when sub-second precision matters.
    """
    if isinstance(value, str):
        return value
    return f"{int(value)}s"


if "__main__" == __name__:
    import os

    os.environ["DATABRICKS_CONFIG_PROFILE"] = "RACETRAC-DEV"
    dtable = describe_table_extended("databricks_demos.rtswv3.racetrac_kpi_metrics")
    # print(dtable.model_dump_json())
    print(json.dumps(dtable.metric_view_definition, indent=2))
