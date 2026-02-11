import importlib

import pytest


@pytest.fixture(scope="session")
def dbx_ai_memory_path(tmp_path_factory):
    """
    Stable directory for this test process.

    Chroma uses a shared process-wide system, so the path must be stable within a single
    Python process.
    """
    return tmp_path_factory.mktemp("dbx_ai_memory")


@pytest.fixture()
def memory_module(dbx_ai_memory_path, monkeypatch):
    monkeypatch.setenv("DBX_AI_MEMORY_PATH", str(dbx_ai_memory_path))

    from dbx_ai import memory  # imported after env var set

    return importlib.reload(memory)


def test_kv_round_trip(memory_module):
    key = ("email", 123, {"a": 1})
    value = {"answer": ["one", 2, 3], "ok": True}

    memory_module.kv_write("tone", key, value)
    got = memory_module.kv_read("tone", key)

    assert got == value


def test_kv_miss_returns_none(memory_module):
    assert memory_module.kv_read("email", "missing") is None
