from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, TypeVar

import reggie_core.paths as paths
from diskcache import Cache
from fasteners import InterProcessLock
from reggie_core import objects, strs

"""
Disk backed cache utilities built on diskcache with inter process locking.
Provides a typed wrapper for cached values and a cache subclass that supports
compute or load semantics with expiration and concurrency control.
"""

T = TypeVar("T")


@dataclass
class DiskCacheValue[T]:
    """Encapsulates a cached value and the timestamp when it was loaded."""

    value: T = field(default=None)
    load_timestamp: datetime = field(default_factory=datetime.now)


class DiskCache(Cache):
    """Cache with on demand loading, optional expiry, and inter process locking."""

    _SENTINEL = object()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Resolve and ensure the backing directory exists
        directory = paths.path(kwargs.get("directory"))
        if not directory:
            lock_dir_name = "_".join(strs.tokenize(DiskCache.__name__))
            directory = paths.temp_dir() / lock_dir_name
        self._lock_directory = directory / ".key-locks"
        self._lock_directory.mkdir(parents=True, exist_ok=True)

    def get_or_load(
        self,
        key: Any,
        loader: Callable[[...], T],
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
            lock_path = self._lock_directory / f"{objects.hash(key).hexdigest()}.lock"
            lock = InterProcessLock(lock_path)
            with lock:
                # Re read in case another process populated while we waited
                cache_value = super().get(key, default=DiskCache._SENTINEL)
                if not DiskCache._is_valid(cache_value, expire):
                    # Load fresh value using the provided loader
                    value = objects.call(loader, key)
                    # Wrap with timestamp for later staleness checks
                    cache_value = DiskCacheValue(value=value)
                    # Persist with expiry so future calls can reuse it
                    super().set(key, cache_value, expire=expire)
        return cache_value

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
    directory = paths.temp_dir() / "test_cache_v3"
    print(f"Cache directory: {directory}")
    cache = DiskCache(directory=directory)

    result = cache.get_or_load("test", loader=lambda: "this is a value", expire=10)
    print(result)
    print(result.load_timestamp.strftime("%Y-%m-%d %H:%M:%S"))
