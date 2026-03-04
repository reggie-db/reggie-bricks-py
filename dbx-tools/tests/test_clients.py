from types import SimpleNamespace

from dbx_tools import clients


def test_spark_prefers_ipython_spark(monkeypatch):
    sentinel = object()
    monkeypatch.setattr(
        clients.runtimes, "ipython_user_ns", lambda key, default=None: sentinel
    )
    assert clients.spark() is sentinel


def test_spark_returns_none_when_connect_disabled(monkeypatch):
    monkeypatch.setattr(
        clients.runtimes, "ipython_user_ns", lambda key, default=None: None
    )
    monkeypatch.setattr(clients.runtimes, "version", lambda: None)
    monkeypatch.setattr(
        clients,
        "_spark",
        lambda: (_ for _ in ()).throw(AssertionError("_spark should not be called")),
    )
    assert clients.spark(connect=False) is None


def test_workspace_client_uses_provided_config(monkeypatch):
    captured = {}
    sentinel_client = object()

    def _workspace_client(**kwargs):
        captured.update(kwargs)
        return sentinel_client

    monkeypatch.setattr(clients, "WorkspaceClient", _workspace_client)
    config = SimpleNamespace()

    result = clients.workspace_client(config=config)

    assert result is sentinel_client
    assert captured.get("config") is config
