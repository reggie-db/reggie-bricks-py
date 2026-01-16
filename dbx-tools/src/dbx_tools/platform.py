import builtins
import functools
import threading
from typing import Any, Callable, TypeVar

T = TypeVar("T")
_INSTANCE_CACHE: dict[str, Any] = {}
_INSTANCE_LOCK = threading.RLock()


def instance(name: str, factory: Callable[[], T] | None) -> T | None:
    def _instance(locked: bool = False) -> T:
        result: T | None = _INSTANCE_CACHE.get(name, None)
        if result is None and not locked:
            user_ns = _ipython_user_ns()
            if user_ns is not None:
                result = user_ns.get(name, None)
            if result is None:
                result = getattr(builtins, name, None)
        if result is None and factory is not None:
            if not locked:
                _INSTANCE_LOCK.acquire()
                try:
                    return _instance(True)
                finally:
                    _INSTANCE_LOCK.release()
            result = factory()
            if result is not None:
                _INSTANCE_CACHE[name] = result
        return result

    return _instance()


def _ipython_user_ns() -> dict[str, Any] | None:
    if get_ipython_fn := _get_ipython_fn():
        if ipython := get_ipython_fn():
            return ipython.user_ns
    return None


@functools.cache
def _get_ipython_fn():
    """Return the ``get_ipython`` callable when IPython is importable."""
    try:
        from IPython import get_ipython  # pyright: ignore[reportMissingImports]

        return get_ipython
    except ImportError:
        pass
