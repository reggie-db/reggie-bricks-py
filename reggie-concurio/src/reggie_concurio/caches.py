import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, TypeVar, Generic

import reggie_core.paths as paths
from diskcache import Cache
from fasteners import InterProcessLock
from reggie_core import logs, objects, strs

LOG = logs.logger(__file__)

"""
Disk backed cache utilities built on diskcache with inter process locking.
Provides a typed wrapper for cached values and a cache subclass that supports
compute or load semantics with expiration and concurrency control.
"""

T = TypeVar("T")


@dataclass
class DiskCacheValue(Generic[T]):
    """Encapsulates a cached value and the timestamp when it was loaded."""

    value: T = field(default=None)
    load_timestamp: datetime = field(default_factory=datetime.now)


class DiskCache(Cache):
    """Cache with on demand loading, optional expiry, and inter process locking."""

    _SENTINEL = object()

    def __init__(self, *args, name: str | None = None, key_lock=True, **kwargs):
        args, kwargs = DiskCache._modify_kwargs(args, name, kwargs)
        super().__init__(*args, **kwargs)
        if key_lock:
            directory = paths.path(super().directory)
            self._lock_directory = directory / ".key-locks"
        else:
            self._lock_directory = None

    def get_or_load(
        self,
        key: Any,
        loader: Callable[[...], T] | Callable[[], T],
        expire: float | None = None,
    ) -> DiskCacheValue[T]:
        """Get a value from disk or load it with the provided loader.

        Parameters
        - key: cache key used to read and write values
        - loader: callable that computes the value when cache is missing or stale
        - expire: seconds before a cached value is considered invalid

        Returns
        - DiskCacheValue holding the value and its load timestamp
        """
        # Fast path read from cache to avoid locking when possible
        cache_value = super().get(key, default=DiskCache._SENTINEL)
        # If missing or stale, prepare to load under a process wide lock
        if not DiskCache._is_valid(cache_value, expire):
            # Derive a lock file path unique to the key
            if self._lock_directory is not None:
                lock_path = (
                    self._lock_directory / f"{objects.hash(key).hexdigest()}.lock"
                )
                lock = InterProcessLock(lock_path)
            else:
                lock = None
            if lock is not None:
                lock.acquire()
            try:
                # Re read in case another process populated while we waited
                cache_value = super().get(key, default=DiskCache._SENTINEL)
                if not DiskCache._is_valid(cache_value, expire):
                    # Load fresh value using the provided loader
                    value = objects.call(loader, key)
                    # Wrap with timestamp for later staleness checks
                    cache_value = DiskCacheValue(value=value)
                    # Persist with expiry so future calls can reuse it
                    super().set(key, cache_value, expire=expire)
            finally:
                if lock is not None:
                    lock.release()
        return cache_value

    @staticmethod
    def _modify_kwargs(args, name: str | None, kwargs):
        if args:
            directory = args[0]
            args = args[1:]
        else:
            directory = kwargs.get("directory")
        directory = paths.path(directory, absolute=True)
        is_file = directory is not None and directory.is_file()
        directory_paths = []
        if is_file or directory is None:
            directory_paths.append(paths.temp_dir())
            if is_file:
                directory_paths.append(
                    f"{directory.stem}_{objects.hash(str(directory), hash_fn=hashlib.md5).hexdigest()}"
                )
            else:
                directory_paths.append("_".join(strs.tokenize(DiskCache.__name__)))
        else:
            directory_paths.append(directory)
        directory_paths.append(name)
        kwargs["directory"] = str(paths.path(*directory_paths, mkdir=True))
        LOG.debug(f"Cache kwargs: {kwargs}")
        return args, kwargs

    @staticmethod
    def _is_valid(cache_value, expire: float | None = None) -> bool:
        """Return True if a cached value exists and is not stale."""
        return cache_value is not DiskCache._SENTINEL and (
            expire is None
            or (
                cache_value.load_timestamp
                >= (datetime.now() - timedelta(seconds=expire))
            )
        )


if __name__ == "__main__":
    cache = DiskCache(directory=__file__)
    LOG.info(f"Cache directory: {cache.directory}")
    result = cache.get_or_load("test", loader=lambda: "this is a value", expire=10)
    LOG.info(f"Cache result: {result}")
    LOG.info(f"Load timestamp: {result.load_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
