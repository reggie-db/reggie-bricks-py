import types

import pytest


class _DummyOAuthCredentialsProvider:
    pass


class _FakeConfig:
    def __init__(
        self,
        *,
        token=None,
        oauth_token_value=None,
        header_factory=None,
        cluster_id=None,
        serverless_compute_id=None,
    ):
        self.token = token
        self._oauth_token_value = oauth_token_value
        self._header_factory = header_factory
        self.cluster_id = cluster_id
        self.serverless_compute_id = serverless_compute_id

    def oauth_token(self):
        if self._oauth_token_value is None:
            raise ValueError("oauth token missing")
        return types.SimpleNamespace(access_token=self._oauth_token_value)


class _FakeWorkspaceClient:
    def __init__(self, config):
        self.config = config


def test_configs_get_delegates_to_core_and_sets_serverless_auto(monkeypatch):
    from dbx_tools import configs

    calls = {"count": 0}
    delegated_config = _FakeConfig(
        token=None,
        oauth_token_value="oauth-token",
        header_factory=_DummyOAuthCredentialsProvider(),
        cluster_id=None,
        serverless_compute_id=None,
    )

    monkeypatch.setattr(configs.runtimes, "version", lambda: None)
    monkeypatch.setattr(configs.runtimes, "app_info", lambda: None)
    monkeypatch.setattr(configs.imports, "resolve", lambda *_a, **_k: None)
    monkeypatch.setattr(
        configs, "OAuthCredentialsProvider", _DummyOAuthCredentialsProvider
    )

    def _fake_core_get_workspace_client():
        calls["count"] += 1
        return _FakeWorkspaceClient(delegated_config)

    monkeypatch.setattr(
        configs, "_core_get_workspace_client", _fake_core_get_workspace_client
    )
    configs._config.cache_clear()

    cfg1 = configs.get()
    cfg2 = configs.get()

    # Delegated config is reused from the cached loader.
    assert cfg1 is delegated_config
    assert cfg2 is delegated_config
    assert calls["count"] == 1
    # Legacy behavior parity: default compute id is still auto-set.
    assert cfg1.serverless_compute_id == "auto"
    # Token extraction still works with OAuth provider-based config.
    assert configs.token(cfg1) == "oauth-token"


def test_configs_get_runtime_short_circuit_does_not_call_core(monkeypatch):
    from dbx_tools import configs

    monkeypatch.setattr(configs.runtimes, "version", lambda: object())
    monkeypatch.setattr(configs.runtimes, "app_info", lambda: None)
    monkeypatch.setattr(
        configs,
        "_core_get_workspace_client",
        lambda: (_ for _ in ()).throw(
            AssertionError("core getter should not be called")
        ),
    )

    fake_runtime_config = _FakeConfig(cluster_id=None, serverless_compute_id=None)
    monkeypatch.setattr(configs, "Config", lambda: fake_runtime_config)

    cfg = configs.get()
    assert cfg is fake_runtime_config
    assert cfg.serverless_compute_id == "auto"


def test_configs_token_prefers_token_then_oauth_callable(monkeypatch):
    from dbx_tools import configs

    monkeypatch.setattr(
        configs, "OAuthCredentialsProvider", _DummyOAuthCredentialsProvider
    )

    cfg_with_token = _FakeConfig(token="pat-token")
    assert configs.token(cfg_with_token) == "pat-token"

    cfg_with_oauth_method = _FakeConfig(token=None, oauth_token_value="oauth-fallback")
    assert configs.token(cfg_with_oauth_method) == "oauth-fallback"


def test_clients_workspace_client_delegates_with_deprecation_warning(monkeypatch):
    from dbx_tools import clients

    expected_client = object()
    monkeypatch.setattr(clients, "_core_get_workspace_client", lambda: expected_client)

    with pytest.warns(DeprecationWarning):
        actual = clients.workspace_client()
    assert actual is expected_client


def test_runtimes_version_is_not_cached(monkeypatch):
    from dbx_tools import runtimes

    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.3")
    v1 = runtimes.version()
    assert str(v1) == "14.3"

    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "15.0")
    v2 = runtimes.version()
    assert str(v2) == "15.0"


def test_catalog_schema_config_is_not_cached(monkeypatch):
    from dbx_tools import catalogs, configs, runtimes

    state = {"catalog_name": "cat_a", "schema_name": "sch_a"}

    def _fake_value(name, default_value=None, **_kwargs):
        if name == "catalog_name":
            return state["catalog_name"]
        if name == "schema_name":
            return state["schema_name"]
        return default_value

    monkeypatch.setattr(configs, "value", _fake_value)
    monkeypatch.setattr(runtimes, "is_pipeline", lambda: False)

    first = catalogs._catalog_schema_config()
    assert first is not None
    assert first.catalog == "cat_a"
    assert first.schema == "sch_a"

    state["catalog_name"] = "cat_b"
    state["schema_name"] = "sch_b"
    second = catalogs._catalog_schema_config()
    assert second is not None
    assert second.catalog == "cat_b"
    assert second.schema == "sch_b"
