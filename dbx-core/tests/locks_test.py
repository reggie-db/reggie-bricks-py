import asyncio
import time

import pytest

from dbx_core import locks
from dbx_core.locks import AsyncFileLock, FileLock, FileLockTimeoutError


def test_file_lock_creates_and_keeps_file(tmp_path):
    lock_path = tmp_path / "nested" / "sample.lock"
    lock = FileLock(lock_path, timeout=1)
    try:
        lock.acquire()
        assert lock_path.exists()
    finally:
        lock.close()

    # Lock file is intentionally not deleted after use.
    assert lock_path.exists()


@pytest.mark.xfail(
    reason="Separate FileLock instances on the same path are not currently re-entrant"
)
def test_file_lock_reentrant_same_thread(tmp_path):
    lock_path = tmp_path / "reentrant.lock"
    outer = FileLock(lock_path, timeout=1)
    inner = FileLock(lock_path, timeout=1)
    try:
        outer.acquire()
        inner.acquire()
    finally:
        inner.close()
        outer.close()

    assert lock_path.exists()


def test_file_lock_timeout_path(monkeypatch, tmp_path):
    lock_path = tmp_path / "timeout.lock"
    monkeypatch.setattr(locks, "_try_lock_fd", lambda fd, shared: False)
    contender = FileLock(lock_path, timeout=0.05, poll_interval=0.01)
    start = time.monotonic()
    try:
        with pytest.raises(FileLockTimeoutError):
            contender.acquire()
    finally:
        contender.close()

    assert time.monotonic() - start >= 0.04


def test_pathlike_is_supported(tmp_path):
    lock_path = tmp_path / "pathlike.lock"
    lock = FileLock(lock_path, timeout=1)
    try:
        lock.acquire()
        assert lock_path.exists()
    finally:
        lock.close()


def test_async_file_lock_context_and_persistence(tmp_path):
    lock_path = tmp_path / "async.lock"

    async def _run():
        lock = AsyncFileLock(lock_path, timeout=1)
        try:
            await lock.acquire()
            assert lock_path.exists()
        finally:
            await lock.aclose()

    asyncio.run(_run())
    assert lock_path.exists()


def test_async_file_lock_timeout_while_sync_holds_lock(tmp_path):
    lock_path = tmp_path / "async-timeout.lock"

    async def _contend():
        contender = AsyncFileLock(lock_path, timeout=0.05, poll_interval=0.01)
        try:
            with pytest.raises(FileLockTimeoutError):
                await contender.acquire()
        finally:
            await contender.aclose()

    original_try = locks._try_lock_fd
    locks._try_lock_fd = lambda fd, shared: False
    try:
        asyncio.run(_contend())
    finally:
        locks._try_lock_fd = original_try


@pytest.mark.xfail(
    reason="close() can raise KeyError after internal mem-lock state is already dropped"
)
def test_file_lock_close_is_not_always_idempotent(tmp_path):
    lock_path = tmp_path / "close-idempotent.lock"
    lock = FileLock(lock_path, timeout=0.01, poll_interval=0.005)
    lock.close()
    lock.close()
