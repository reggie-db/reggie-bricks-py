import types


def test_get_falls_back_to_apps_oauth_env(monkeypatch):
    """
    Verify `configs.get()` delegates default auth loading to core workspace client config.

    This test is fully offline: it mocks the core workspace client factory so no real
    auth calls are made.
    """

    from dbx_tools import configs

    # Ensure we don't short-circuit to `Config()` due to runtime detection.
    monkeypatch.setattr(configs.runtimes, "version", lambda: None)
    monkeypatch.setattr(configs.runtimes, "app_info", lambda: None)

    # Ensure `_config()` returns the concrete object, not a LazyObjectProxy wrapper.
    monkeypatch.setattr(configs.imports, "resolve", lambda *_a, **_k: None)

    monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "app-sp-client-id")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "app-sp-client-secret")

    class DummyOAuthCredentialsProvider:
        pass

    monkeypatch.setattr(
        configs, "OAuthCredentialsProvider", DummyOAuthCredentialsProvider
    )

    class FakeConfig:
        def __init__(self, **kwargs):
            self._kwargs = kwargs
            self.cluster_id = None
            self.serverless_compute_id = None
            self._header_factory = DummyOAuthCredentialsProvider()
            self.token = None

        def oauth_token(self):
            return types.SimpleNamespace(access_token="env-oauth-token")

        @property
        def host(self):
            return self._kwargs.get("host")

    class FakeWorkspaceClient:
        def __init__(self, config):
            self.config = config

    fake_cfg = FakeConfig(
        host="https://example.cloud.databricks.com",
        client_id="app-sp-client-id",
        client_secret="app-sp-client-secret",
        auth_type="oauth-m2m",
    )
    monkeypatch.setattr(
        configs, "_core_get_workspace_client", lambda: FakeWorkspaceClient(fake_cfg)
    )

    # Avoid cross-test cache effects in this module.
    configs._config.cache_clear()

    cfg = configs.get()
    assert isinstance(cfg, FakeConfig)
    assert cfg.host == "https://example.cloud.databricks.com"
    assert cfg._kwargs["client_id"] == "app-sp-client-id"
    assert cfg._kwargs["client_secret"] == "app-sp-client-secret"
    assert cfg._kwargs["auth_type"] == "oauth-m2m"

    # The loader sets serverless compute to auto when neither compute id is present.
    assert cfg.serverless_compute_id == "auto"

    # Token extraction should work for the env-based config.
    assert configs.token(cfg) == "env-oauth-token"
