import os

import pytest

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
    calls = {
        "caddy_config": None,
        "caddy_flags": None,
        "starts": [],
        "stops": 0,
    }

    class _FakeProc:
        def __init__(self, pid, poll_values=None):
            self.pid = pid
            self._poll_values = poll_values or []
            self._idx = 0

        def poll(self):
            if self._idx < len(self._poll_values):
                v = self._poll_values[self._idx]
                self._idx += 1
                return v
            return None

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

    backend_proc = _FakeProc(os.getpid() + 1, poll_values=[None, 17])
    frontend_proc = _FakeProc(os.getpid() + 2, poll_values=[None, None, None])
    caddy_proc = _FakeProc(os.getpid() + 3, poll_values=[None, None, None])
    procs = [backend_proc, frontend_proc]

    def _fake_start_reflex(reflex_args, env, *, backend_only, frontend_only):
        calls["starts"].append(
            {
                "args": list(reflex_args),
                "env": dict(env),
                "backend_only": backend_only,
                "frontend_only": frontend_only,
            }
        )
        return procs[len(calls["starts"]) - 1]

    def _fake_stop_reflex(_proc):
        calls["stops"] += 1

    def _fake_wait_until_any_exits(_processes):
        return 0, 17

    def _fake_caddy_enter(self):
        return caddy_proc

    monkeypatch.setattr(run.caddy, "run", _fake_caddy_run)
    monkeypatch.setattr(_FakeCaddyContext, "__enter__", _fake_caddy_enter)
    monkeypatch.setattr(run, "_start_reflex", _fake_start_reflex)
    monkeypatch.setattr(run, "_stop_reflex", _fake_stop_reflex)
    monkeypatch.setattr(run, "_wait_until_any_exits", _fake_wait_until_any_exits)
    monkeypatch.setattr(run.signal, "signal", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(run, "_get_free_port", lambda: 5111)

    exit_code = run.main([])
    assert exit_code == 17
    assert calls["caddy_flags"] == ("watch",)
    assert "handle /_hmr" in str(calls["caddy_config"])
    assert len(calls["starts"]) == 2
    assert calls["starts"][0]["args"] == ["--env", "dev"]
    assert calls["starts"][1]["args"] == ["--env", "dev"]
    assert calls["starts"][0]["backend_only"] is True
    assert calls["starts"][0]["frontend_only"] is False
    assert calls["starts"][1]["backend_only"] is False
    assert calls["starts"][1]["frontend_only"] is True
    assert calls["starts"][0]["env"]["DATABRICKS_APP_PORT"]
    assert calls["starts"][0]["env"]["REFLEX_BACKEND_PORT"]
    assert calls["starts"][0]["env"]["REFLEX_FRONTEND_PORT"]
    assert calls["stops"] == 2


def test_stop_all_rethrows_after_attempting_all_stops(monkeypatch):
    calls = []

    class _FakeProc:
        def __init__(self, idx):
            self.idx = idx

    def _fake_stop_reflex(proc):
        calls.append(proc.idx)
        if proc.idx == 1:
            raise RuntimeError("boom")

    monkeypatch.setattr(run, "_stop_reflex", _fake_stop_reflex)

    with pytest.raises(ExceptionGroup):
        run._stop_all([_FakeProc(0), _FakeProc(1), _FakeProc(2)])

    # All stop attempts should run even when one fails.
    assert calls == [0, 1, 2]
