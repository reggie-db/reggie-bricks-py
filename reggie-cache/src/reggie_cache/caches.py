from ast import TypeVar
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

import reggie_core.paths as paths
from diskcache import Cache
from fasteners import InterProcessLock
from reggie_core import objects

T = TypeVar("T")


@dataclass
class DiskCacheValue[T]:
    value: T = field(default=None)
    load_timestamp: datetime = field(default_factory=datetime.now)


class DiskCache(Cache):
    _SENTINEL = object()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.directory_path = paths.path(kwargs.get("directory"))
        if self.directory_path:
            self.directory_path.mkdir(parents=True, exist_ok=True)

    def get(
        self, key: Any, expire: float | None = None, loader: Callable[[...], Any] = None
    ) -> DiskCacheValue:
        cache_key = objects.hash(key).hexdigest()
        v = super().get(cache_key, default=DiskCache._SENTINEL)
        if v is not DiskCache._SENTINEL:
            return v
        lock_path = self.directory_path / f"{cache_key}.lock"

        lock = InterProcessLock(lock_path)
        with lock:
            v = super().get(key, default=DiskCache._SENTINEL)
            if v is not DiskCache._SENTINEL:
                return v
            value = objects.call(loader, cache_key)
            cache_value = DiskCacheValue(value=value)
            super().set(cache_key, cache_value, expire=expire)
            return cache_value


if __name__ == "__main__":
    cache = DiskCache(directory=paths.temp_dir() / "test_cache_v3")
    result = cache.get("test", loader=lambda: "this is a value")
    print(result)
