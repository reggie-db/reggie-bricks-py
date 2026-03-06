import importlib.util
from importlib.machinery import SourceFileLoader
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_metric_views_module():
    module_path = (
        Path(__file__).resolve().parents[1] / "src" / "dbx_tools" / "metric_views"
    )
    loader = SourceFileLoader("dbx_tools_metric_views", str(module_path))
    spec = importlib.util.spec_from_loader("dbx_tools_metric_views", loader)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class _MockStatementExecution:
    def __init__(self, response):
        self._response = response
        self.execute_calls = []

    def execute_statement(self, **kwargs):
        self.execute_calls.append(kwargs)
        return self._response


class _MockWorkspaceClient:
    def __init__(self, statement_execution):
        self.statement_execution = statement_execution


def test_describe_returns_view_text_from_describe_json():
    metric_views = _load_metric_views_module()
    statement_execution = _MockStatementExecution(
        SimpleNamespace(
            status=SimpleNamespace(
                state=metric_views.StatementState.SUCCEEDED, error=None
            ),
            result=SimpleNamespace(
                data_array=[
                    ['{"view_text":"create materialized view demo as select 1"}']
                ]
            ),
        )
    )
    workspace_client = _MockWorkspaceClient(statement_execution)

    view_text = metric_views.describe(
        "databricks_demos.rtswv3.racetrac_kpi_metrics",
        workspace_client=workspace_client,
        warehouse_id="wh-1",
    )

    assert view_text == "create materialized view demo as select 1"
    assert (
        statement_execution.execute_calls[0]["statement"]
        == "DESCRIBE TABLE EXTENDED databricks_demos.rtswv3.racetrac_kpi_metrics AS JSON"
    )


def test_describe_raises_when_view_text_missing():
    metric_views = _load_metric_views_module()
    statement_execution = _MockStatementExecution(
        SimpleNamespace(
            status=SimpleNamespace(
                state=metric_views.StatementState.SUCCEEDED, error=None
            ),
            result=SimpleNamespace(data_array=[['{"table_name":"x"}']]),
        )
    )
    workspace_client = _MockWorkspaceClient(statement_execution)

    with pytest.raises(RuntimeError, match="missing view_text"):
        metric_views.describe(
            "databricks_demos.rtswv3.racetrac_kpi_metrics",
            workspace_client=workspace_client,
            warehouse_id="wh-1",
        )
