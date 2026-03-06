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


def test_describe_returns_view_text_from_describe_json(monkeypatch):
    metric_views = _load_metric_views_module()
    execute_calls: list[dict] = []
    response = SimpleNamespace(
        result=SimpleNamespace(
            data_array=[['{"view_text":"create materialized view demo as select 1"}']]
        )
    )

    def _mock_execute_statement(**kwargs):
        execute_calls.append(kwargs)
        return response

    monkeypatch.setattr(
        metric_views.warehouses, "execute_statement", _mock_execute_statement
    )
    workspace_client = object()

    view_text = metric_views.describe(
        "databricks_demos.rtswv3.racetrac_kpi_metrics",
        workspace_client=workspace_client,
        warehouse_id="wh-1",
        wait_timeout="10s",
    )

    assert view_text == "create materialized view demo as select 1"
    assert (
        execute_calls[0]["statement"]
        == "DESCRIBE TABLE EXTENDED databricks_demos.rtswv3.racetrac_kpi_metrics AS JSON"
    )
    assert execute_calls[0]["workspace_client"] is workspace_client
    assert execute_calls[0]["warehouse_id"] == "wh-1"
    assert execute_calls[0]["wait_timeout"] == "10s"


def test_describe_raises_when_view_text_missing(monkeypatch):
    metric_views = _load_metric_views_module()
    response = SimpleNamespace(
        result=SimpleNamespace(data_array=[['{"table_name":"x"}']]),
    )

    def _mock_execute_statement(**_kwargs):
        return response

    monkeypatch.setattr(
        metric_views.warehouses, "execute_statement", _mock_execute_statement
    )

    with pytest.raises(RuntimeError, match="describe query failed"):
        metric_views.describe(
            "databricks_demos.rtswv3.racetrac_kpi_metrics",
            workspace_client=object(),
            warehouse_id="wh-1",
        )
