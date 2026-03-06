from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from dbx_tools import warehouses


@dataclass(frozen=True)
class MockWarehouse:
    id: str
    name: str = "test"
    cluster_size: str = "Small"
    enable_serverless_compute: bool = False


class MockWarehouses:
    def __init__(self, items: list[MockWarehouse]):
        self._items = items

    def list(self) -> list[MockWarehouse]:
        return self._items


class MockWorkspaceClient:
    def __init__(self, items: list[MockWarehouse]):
        self.warehouses = MockWarehouses(items)


class MockStatementExecution:
    def __init__(self, responses):
        self._responses = list(responses)
        self.execute_calls = []
        self.get_calls = []

    def execute_statement(self, **kwargs):
        self.execute_calls.append(kwargs)
        return SimpleNamespace(statement_id="stmt-1")

    def get_statement(self, statement_id):
        self.get_calls.append(statement_id)
        return self._responses.pop(0)


class MockWorkspaceClientWithStatements(MockWorkspaceClient):
    def __init__(
        self, items: list[MockWarehouse], statement_execution: MockStatementExecution
    ):
        super().__init__(items)
        self.statement_execution = statement_execution


def test_warehouse_size_rank():
    test_cases = [
        ("Small", -1),
        ("X-Small", -2),
        ("1X-Small", -2),
        ("2X-Small", -3),
        ("Medium", 0),
        ("Large", 1),
        ("X-Large", 2),
        ("2X-Large", 3),
        ("4X-Large", 5),
        ("8X-Large", 9),
        (None, 0),
        ("", 0),
        ("Unknown", 0),
    ]

    for size, expected in test_cases:
        assert warehouses._warehouse_size_rank(size) == expected


def test_warehouse_selection_logic():
    w1 = MockWarehouse(
        id="1", name="A", cluster_size="Small", enable_serverless_compute=False
    )
    w2 = MockWarehouse(
        id="2", name="B", cluster_size="Medium", enable_serverless_compute=False
    )
    w3 = MockWarehouse(
        id="3", name="C", cluster_size="Small", enable_serverless_compute=True
    )
    w4 = MockWarehouse(
        id="4", name="shared", cluster_size="Small", enable_serverless_compute=False
    )

    items = [w1, w2, w3, w4]
    client = MockWorkspaceClient(items)
    assert warehouses.get(workspace_client=client).id == "3"

    items = [w1, w2, w4]
    client = MockWorkspaceClient(items)
    assert warehouses.get(workspace_client=client).id == "2"

    w5 = MockWarehouse(
        id="5", name="tools", cluster_size="Small", enable_serverless_compute=False
    )
    items = [w1, w4, w5]
    client = MockWorkspaceClient(items)
    assert warehouses.get(workspace_client=client).id == "4"

    items = [w1, w5]
    client = MockWorkspaceClient(items)
    assert warehouses.get(workspace_client=client).id == "5"


def test_warehouse_raises_on_empty():
    client = MockWorkspaceClient([])
    with pytest.raises(ValueError, match="No accessible SQL warehouses found"):
        warehouses.get(workspace_client=client)


def test_warehouse_ignores_warehouses_without_id():
    w1 = MockWarehouse(
        id=None, name="No ID", cluster_size="Large", enable_serverless_compute=True
    )
    w2 = MockWarehouse(
        id="2", name="With ID", cluster_size="Small", enable_serverless_compute=False
    )

    client = MockWorkspaceClient([w1, w2])
    assert warehouses.get(workspace_client=client).id == "2"


def test_warehouse_prefers_serverless_then_size_then_name():
    items = [
        MockWarehouse(
            id="1",
            name="Shared Endpoint",
            cluster_size="Small",
            enable_serverless_compute=True,
        ),
        MockWarehouse(
            id="2",
            name="tools",
            cluster_size="X-Large",
            enable_serverless_compute=True,
        ),
        MockWarehouse(
            id="3",
            name="random biggest but not serverless",
            cluster_size="4X-Large",
            enable_serverless_compute=False,
        ),
        MockWarehouse(
            id="4",
            name="zzz serverless biggest",
            cluster_size="2X-Large",
            enable_serverless_compute=True,
        ),
        MockWarehouse(
            id="5",
            name="Shared Endpoint XL",
            cluster_size="X-Large",
            enable_serverless_compute=True,
        ),
    ]

    chosen = warehouses.get(workspace_client=MockWorkspaceClient(items))
    assert chosen.id == "4"


def test_warehouse_name_preference_breaks_size_ties_within_serverless():
    items = [
        MockWarehouse(
            id="1",
            name="zz top",
            cluster_size="X-Large",
            enable_serverless_compute=True,
        ),
        MockWarehouse(
            id="2",
            name="tools",
            cluster_size="X-Large",
            enable_serverless_compute=True,
        ),
        MockWarehouse(
            id="3",
            name="Shared Endpoint",
            cluster_size="X-Large",
            enable_serverless_compute=True,
        ),
    ]

    chosen = warehouses.get(workspace_client=MockWorkspaceClient(items))
    assert chosen.id == "3"


def test_list_name_preference_uses_tokenized_contains_matching():
    items = [
        MockWarehouse(
            id="1",
            name="Shared Endpoint XL",
            cluster_size="X-Large",
            enable_serverless_compute=True,
        ),
        MockWarehouse(
            id="2",
            name="Demo Endpoint",
            cluster_size="X-Large",
            enable_serverless_compute=True,
        ),
    ]

    ranked = warehouses.list(
        warehouses.WarehouseSort.NAME,
        name_preference=["endpoint", "tools"],
        workspace_client=MockWorkspaceClient(items),
    )
    assert ranked[0].id == "1"


def test_warehouse_raises_when_none_found():
    class _EmptyWC:
        class _W:
            @staticmethod
            def list():
                return []

        warehouses = _W()

    with pytest.raises(ValueError):
        warehouses.get(workspace_client=_EmptyWC())


def test_execute_statement_polls_until_succeeded():
    statement_execution = MockStatementExecution(
        [
            SimpleNamespace(
                status=SimpleNamespace(
                    state=warehouses.StatementState.RUNNING,
                    error=None,
                ),
                result=None,
            ),
            SimpleNamespace(
                status=SimpleNamespace(
                    state=warehouses.StatementState.SUCCEEDED,
                    error=None,
                ),
                result=SimpleNamespace(data_array=[["ok"]]),
            ),
        ]
    )
    wc = MockWorkspaceClientWithStatements(
        [
            MockWarehouse(
                id="wh1",
                name="shared",
                cluster_size="Small",
                enable_serverless_compute=True,
            )
        ],
        statement_execution,
    )

    response = warehouses.execute_statement(
        statement="SELECT 1",
        workspace_client=wc,
        poll_interval_seconds=0,
    )

    assert response.status.state == warehouses.StatementState.SUCCEEDED
    assert statement_execution.execute_calls[0]["warehouse_id"] == "wh1"
    assert statement_execution.execute_calls[0]["statement"] == "SELECT 1"
    assert statement_execution.get_calls == ["stmt-1", "stmt-1"]


def test_execute_statement_raises_on_failed_terminal_state():
    statement_execution = MockStatementExecution(
        [
            SimpleNamespace(
                status=SimpleNamespace(
                    state=warehouses.StatementState.FAILED,
                    error=SimpleNamespace(message="bad sql"),
                ),
                result=None,
            )
        ]
    )
    wc = MockWorkspaceClientWithStatements(
        [
            MockWarehouse(
                id="wh1",
                name="shared",
                cluster_size="Small",
                enable_serverless_compute=True,
            )
        ],
        statement_execution,
    )

    with pytest.raises(RuntimeError, match="bad sql"):
        warehouses.execute_statement(
            statement="SELECT bad",
            workspace_client=wc,
            poll_interval_seconds=0,
        )
