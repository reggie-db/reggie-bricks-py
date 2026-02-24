import types


def test_get_falls_back_to_apps_oauth_env(monkeypatch):
    """
    Verify the default config loader falls back to Databricks Apps OAuth env vars when
    no profile/default config is available.

    This test is fully offline: it mocks the Databricks SDK `Config` type so no real
    auth calls are made.
    """

    from dbx_tools import configs

    # Ensure we don't short-circuit to `Config()` due to runtime detection.
    monkeypatch.setattr(configs.runtimes, "version", lambda: None)
    monkeypatch.setattr(configs.runtimes, "app_info", lambda: None)

    # Ensure `_config()` returns the concrete object, not a LazyObjectProxy wrapper.
    monkeypatch.setattr(configs.imports, "resolve", lambda *_a, **_k: None)

    # Ensure we don't try to use any .databrickscfg profile.
    monkeypatch.setattr(configs, "_databricks_config_profile", lambda: None)

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

    def _fake_config_factory(*_args, **kwargs):
        # Simulate "no default config present" for `Config()`.
        if "host" not in kwargs:
            raise ValueError("Config not found")
        return FakeConfig(**kwargs)

    monkeypatch.setattr(configs, "Config", _fake_config_factory)

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
