import os

import pytest

from dbx_reflex import run


def test_resolve_app_port_prefers_cli_value(monkeypatch):
    monkeypatch.setenv("DATABRICKS_APP_PORT", "9100")
    parsed = run._parse_args(["--app-port", "8010"])
    assert run._resolve_app_port(parsed) == 8010


def test_resolve_app_port_uses_env_when_cli_missing(monkeypatch):
    monkeypatch.setenv("DATABRICKS_APP_PORT", "9100")
    parsed = run._parse_args([])
    assert run._resolve_app_port(parsed) == 9100


def test_normalize_reflex_args_defaults_when_empty():
    assert run._normalize_reflex_args([]) == ["--env", "dev"]
    assert run._normalize_reflex_args(["--"]) == ["--env", "dev"]


def test_normalize_reflex_args_passes_through_custom_values():
    args = ["--env", "prod"]
    assert run._normalize_reflex_args(args) == args


def test_main_sets_env_and_runs_reflex(monkeypatch):
    calls = {
        "start": None,
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

    reflex_proc = _FakeProc(os.getpid() + 1, poll_values=[None, 17])

    def _fake_start_reflex(reflex_args, env):
        calls["start"] = {"args": list(reflex_args), "env": dict(env)}
        return reflex_proc

    def _fake_stop_process(_proc):
        calls["stops"] += 1

    def _fake_wait_until_any_exits(processes):
        assert processes == [reflex_proc]
        return 0, 17

    monkeypatch.setattr(run, "_start_reflex", _fake_start_reflex)
    monkeypatch.setattr(run, "_stop_process", _fake_stop_process)
    monkeypatch.setattr(run, "_wait_until_any_exits", _fake_wait_until_any_exits)
    monkeypatch.setattr(run.signal, "signal", lambda *_args, **_kwargs: None)

    exit_code = run.main([])
    assert exit_code == 17
    assert calls["start"]["args"] == ["--env", "dev"]
    assert calls["start"]["env"]["DATABRICKS_APP_PORT"]
    assert "REFLEX_BACKEND_PORT" not in calls["start"]["env"]
    assert "REFLEX_FRONTEND_PORT" not in calls["start"]["env"]
    assert calls["stops"] == 1


def test_stop_all_rethrows_after_attempting_all_stops(monkeypatch):
    calls = []

    class _FakeProc:
        def __init__(self, idx):
            self.idx = idx

    def _fake_stop_process(proc):
        calls.append(proc.idx)
        if proc.idx == 1:
            raise RuntimeError("boom")

    monkeypatch.setattr(run, "_stop_process", _fake_stop_process)

    with pytest.raises(ExceptionGroup):
        run._stop_all([_FakeProc(0), _FakeProc(1), _FakeProc(2)])

    # All stop attempts should run even when one fails.
    assert calls == [0, 1, 2]
