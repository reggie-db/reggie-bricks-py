import importlib
from functools import lru_cache
from types import ModuleType
from typing import Any

"""
Module for resolving python modules and attributes.
"""


def resolve(
    name: str, attr: str | None = None, package: str | None = None, cache: bool = True
) -> Any | None:
    """
    Resolve a module or an attribute within a module.

    Args:
        name: The name of the module.
        attr: The name of the attribute within the module.
        package: The package name if resolving a relative import.
        cache: Whether to use the cached version of the module.

    Returns:
        The resolved attribute or module, or None if not found.
    """
    if module_type := resolve_module(name, package, cache):
        return getattr(module_type, attr, None) if attr else module_type
    return None


def resolve_module(
    name: str, package: str | None = None, cache: bool = True
) -> ModuleType | None:
    """
    Resolve a module by name.

    Args:
        name: The name of the module to resolve.
        package: The package name if resolving a relative import.
        cache: Whether to use the cached version of the module.

    Returns:
        The resolved module or None if not found.

    Raises:
        ValueError: If the module name is empty.
    """
    if not name:
        raise ValueError(f"Invalid module name: {name}")
    return (
        _resolve_module_cached(name, package)
        if cache
        else _resolve_module(name, package)
    )


def _resolve_module(name: str, package: str | None) -> ModuleType | None:
    """
    Internal helper to resolve a module using importlib.
    """
    # noinspection PyBroadException
    try:
        return importlib.import_module(name, package)
    except ModuleNotFoundError:
        pass
    except ImportError:
        pass
    return None


@lru_cache(maxsize=None)
def _resolve_module_cached(name: str, package: str | None) -> ModuleType | None:
    """
    Internal helper to resolve a module with caching.
    """
    return _resolve_module(name, package)
