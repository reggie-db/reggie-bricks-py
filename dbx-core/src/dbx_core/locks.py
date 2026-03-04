"""
Cross-platform (macOS, Linux, Windows) file lock usable as a context manager
for both sync and async code.

Guarantees:
- If the lock file does not exist, it is created.
- The lock file is never deleted; it remains after use.
- In-process coordination: per-path in-memory locking so multiple threads
  in the same process do not race to acquire the OS file lock.

Notes:
- OS lock is advisory (standard OS file locks).
- Re-entrancy via separate lock instances on the same path is not guaranteed.

Usage (sync):
    from dbx_core.locks import FileLock

    with FileLock("my.lock", timeout=10):
        ...

Usage (async):
    from dbx_core.locks import AsyncFileLock

    async with AsyncFileLock("my.lock", timeout=10):
        ...
"""

import asyncio
import os
import threading
import time
from dataclasses import dataclass
from os import PathLike
from typing import Optional


class FileLockTimeoutError(TimeoutError):
    """Raised when a lock cannot be acquired before the configured timeout."""

    pass


@dataclass
class _LockState:
    """Internal lock acquisition state shared across helper calls."""

    path: PathLike
    timeout: Optional[float]
    poll_interval: float
    shared: bool
    fd: Optional[int] = None


# ---------------------------
# In-process coordination
# ---------------------------

_MEM_GUARD = threading.RLock()
# path -> (RLock, refcount)
_MEM_LOCKS: dict[str, tuple[threading.RLock, int]] = {}


def _norm_path(path: PathLike) -> str:
    """Expand and normalize a lock path to an absolute path."""
    return os.path.abspath(os.path.expanduser(os.fspath(path)))


def _acquire_mem_lock(path: PathLike) -> threading.RLock:
    """Get or create an in-process reentrant lock for ``path`` and bump refcount."""
    p = _norm_path(path)
    with _MEM_GUARD:
        entry = _MEM_LOCKS.get(p)
        if entry is None:
            lock = threading.RLock()
            _MEM_LOCKS[p] = (lock, 1)
            return lock
        lock, n = entry
        _MEM_LOCKS[p] = (lock, n + 1)
        return lock


def _release_mem_lock(path: PathLike) -> None:
    """Decrease in-process lock refcount for ``path`` and drop when unused."""
    p = _norm_path(path)
    with _MEM_GUARD:
        lock, n = _MEM_LOCKS[p]
        if n <= 1:
            _MEM_LOCKS.pop(p, None)
        else:
            _MEM_LOCKS[p] = (lock, n - 1)


# ---------------------------
# File operations
# ---------------------------


def _ensure_parent_dir(path: PathLike) -> None:
    """Create parent directories for ``path`` when missing."""
    parent = os.path.dirname(os.path.abspath(os.fspath(path)))
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)


def _open_lock_file(path: PathLike) -> int:
    """Open or create the lock file and return an OS file descriptor."""
    _ensure_parent_dir(path)
    flags = os.O_CREAT | os.O_RDWR
    return os.open(os.fspath(path), flags, 0o666)


# ---------------------------
# OS-specific locking
# ---------------------------

if os.name == "nt":
    import msvcrt

    def _lock_fd(fd: int, shared: bool) -> None:
        # msvcrt.locking is mandatory byte-range locking and blocks.
        # Lock 1 byte from current position. Ensure we are at 0.
        os.lseek(fd, 0, os.SEEK_SET)
        mode = msvcrt.LK_RLCK if shared else msvcrt.LK_LOCK
        msvcrt.locking(fd, mode, 1)

    def _try_lock_fd(fd: int, shared: bool) -> bool:
        os.lseek(fd, 0, os.SEEK_SET)
        mode = msvcrt.LK_NBRLCK if shared else msvcrt.LK_NBLCK
        try:
            msvcrt.locking(fd, mode, 1)
            return True
        except OSError:
            return False

    def _unlock_fd(fd: int) -> None:
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)

else:
    import fcntl

    def _lock_fd(fd: int, shared: bool) -> None:
        op = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
        fcntl.flock(fd, op)

    def _try_lock_fd(fd: int, shared: bool) -> bool:
        op = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
        try:
            fcntl.flock(fd, op | fcntl.LOCK_NB)
            return True
        except BlockingIOError:
            return False

    def _unlock_fd(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_UN)


def _acquire_os_lock(state: _LockState) -> None:
    """Acquire the platform file lock, polling until acquired or timed out."""
    fd = _open_lock_file(state.path)
    state.fd = fd

    deadline: Optional[float] = None
    if state.timeout is not None:
        deadline = time.monotonic() + float(state.timeout)

    try:
        while True:
            if _try_lock_fd(fd, state.shared):
                return

            if deadline is not None and time.monotonic() >= deadline:
                raise FileLockTimeoutError(
                    f"Timed out acquiring lock for {state.path!r} after {state.timeout}s"
                )

            time.sleep(state.poll_interval)
    except Exception:
        try:
            os.close(fd)
        finally:
            state.fd = None
        raise


def _release_os_lock(state: _LockState) -> None:
    """Release the platform file lock and close the tracked descriptor."""
    fd = state.fd
    if fd is None:
        return
    try:
        _unlock_fd(fd)
    finally:
        os.close(fd)
        state.fd = None


# ---------------------------
# Public API
# ---------------------------


class FileLock:
    """
    Sync file lock context manager with in-process coordination.

    Args:
        path: Lock file path.
        timeout: Seconds to wait; None waits forever.
        poll_interval: Sleep between retries for non-blocking attempts.
        shared: If True, shared/read lock when supported. Default exclusive.
    """

    def __init__(
        self,
        path: PathLike,
        *,
        timeout: Optional[float] = None,
        poll_interval: float = 0.1,
        shared: bool = False,
    ) -> None:
        self._mem_path = _norm_path(path)
        self._mem_lock = _acquire_mem_lock(self._mem_path)
        self._state = _LockState(
            path=self._mem_path,
            timeout=timeout,
            poll_interval=poll_interval,
            shared=shared,
        )
        self._mem_acquired = False

    def acquire(self) -> None:
        if not self._mem_acquired:
            self._mem_lock.acquire()
            self._mem_acquired = True
        _acquire_os_lock(self._state)

    def release(self) -> None:
        try:
            _release_os_lock(self._state)
        finally:
            if self._mem_acquired:
                try:
                    self._mem_lock.release()
                finally:
                    self._mem_acquired = False

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()

    def __del__(self) -> None:
        # Best-effort cleanup if object is GC'd while held.
        try:
            self.release()
        except Exception:
            pass
        try:
            _release_mem_lock(self._mem_path)
        except Exception:
            pass

    def close(self) -> None:
        self.release()
        _release_mem_lock(self._mem_path)


class AsyncFileLock:
    """
    Async file lock context manager with in-process coordination.

    Implementation:
    - Acquires an in-memory threading.RLock in a worker thread to serialize
      cross-thread attempts in the same process.
    - Acquires the OS file lock in a worker thread to avoid blocking the loop.
    """

    def __init__(
        self,
        path: PathLike,
        *,
        timeout: Optional[float] = None,
        poll_interval: float = 0.1,
        shared: bool = False,
    ) -> None:
        self._mem_path = _norm_path(path)
        self._mem_lock = _acquire_mem_lock(self._mem_path)
        self._state = _LockState(
            path=self._mem_path,
            timeout=timeout,
            poll_interval=poll_interval,
            shared=shared,
        )
        self._mem_acquired = False

    async def acquire(self) -> None:
        # Acquire in-memory lock without blocking event loop.
        if not self._mem_acquired:
            await asyncio.to_thread(self._mem_lock.acquire)
            self._mem_acquired = True
        await asyncio.to_thread(_acquire_os_lock, self._state)

    async def release(self) -> None:
        try:
            await asyncio.to_thread(_release_os_lock, self._state)
        finally:
            if self._mem_acquired:
                try:
                    await asyncio.to_thread(self._mem_lock.release)
                finally:
                    self._mem_acquired = False

    async def __aenter__(self) -> "AsyncFileLock":
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.release()

    def __del__(self) -> None:
        # Best-effort cleanup; cannot await here.
        try:
            if self._state.fd is not None:
                _release_os_lock(self._state)
        except Exception:
            pass
        try:
            if self._mem_acquired:
                self._mem_lock.release()
                self._mem_acquired = False
        except Exception:
            pass
        try:
            _release_mem_lock(self._mem_path)
        except Exception:
            pass

    async def aclose(self) -> None:
        await self.release()
        _release_mem_lock(self._mem_path)
