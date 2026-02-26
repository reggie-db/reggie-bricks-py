import os

from dbx_reflex import run


def test_caddyfile_contains_hmr_and_websocket_routes():
    data = run._caddyfile(app_port=8000, backend_port=5000, frontend_port=5173)
    assert ":8000 {" in data
    assert "handle /_hmr" in data
    assert "reverse_proxy localhost:5173" in data
    assert "handle /_upload" in data
    assert "reverse_proxy localhost:5000" in data
    assert "reverse_proxy @websockets localhost:5000" in data


def test_normalize_reflex_args_defaults_when_empty():
    assert run._normalize_reflex_args([]) == [
        "--env",
        "dev",
        "--backend-only",
        "false",
    ]
    assert run._normalize_reflex_args(["--"]) == [
        "--env",
        "dev",
        "--backend-only",
        "false",
    ]


def test_normalize_reflex_args_passes_through_custom_values():
    args = ["--env", "prod", "--backend-only", "true"]
    assert run._normalize_reflex_args(args) == args


def test_resolve_ports_prefers_cli_values(monkeypatch):
    monkeypatch.setenv("DATABRICKS_APP_PORT", "9000")
    monkeypatch.setenv("REFLEX_BACKEND_PORT", "9001")
    monkeypatch.setenv("REFLEX_FRONTEND_PORT", "9002")

    parsed = run._parse_args(
        [
            "--app-port",
            "8010",
            "--backend-port",
            "5010",
            "--frontend-port",
            "5180",
        ]
    )
    app_port, backend_port, frontend_port = run._resolve_ports(parsed)
    assert app_port == 8010
    assert backend_port == 5010
    assert frontend_port == 5180


def test_resolve_ports_randomizes_backend_and_frontend_by_default(monkeypatch):
    monkeypatch.setenv("DATABRICKS_APP_PORT", "9100")
    monkeypatch.setenv("REFLEX_BACKEND_PORT", "9001")
    monkeypatch.setenv("REFLEX_FRONTEND_PORT", "9002")

    free_ports = iter([5011, 5179])
    monkeypatch.setattr(run, "_get_free_port", lambda: next(free_ports))
    parsed = run._parse_args([])
    app_port, backend_port, frontend_port = run._resolve_ports(parsed)
    assert app_port == 9100
    assert backend_port == 5011
    assert frontend_port == 5179
    assert backend_port != frontend_port


def test_main_sets_env_and_uses_caddy_and_reflex(monkeypatch):
    calls = {"caddy_config": None, "caddy_flags": None, "cmd": None, "env": None}

    class _FakeProc:
        pid = os.getpid()

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    class _FakeCaddyContext:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb):
            return False

    def _fake_caddy_run(config, *flags, **kwargs):
        calls["caddy_config"] = config
        calls["caddy_flags"] = flags
        return _FakeCaddyContext()

    def _fake_start_reflex(reflex_args, env):
        calls["cmd"] = list(reflex_args)
        calls["env"] = dict(env)
        return _FakeProc()

    monkeypatch.setattr(run.caddy, "run", _fake_caddy_run)
    monkeypatch.setattr(run, "_start_reflex", _fake_start_reflex)
    monkeypatch.setattr(run, "_stop_reflex", lambda _proc: None)
    monkeypatch.setattr(run.signal, "signal", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(run, "_get_free_port", lambda: 5111)

    exit_code = run.main([])
    assert exit_code == 0
    assert calls["caddy_flags"] == ("watch",)
    assert "handle /_hmr" in str(calls["caddy_config"])
    assert calls["cmd"] == ["--env", "dev", "--backend-only", "false"]
    assert calls["env"]["DATABRICKS_APP_PORT"]
    assert calls["env"]["REFLEX_BACKEND_PORT"]
    assert calls["env"]["REFLEX_FRONTEND_PORT"]
