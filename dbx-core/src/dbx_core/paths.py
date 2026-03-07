"""Path helpers for resolving, validating, and locating common directories."""

import functools
import getpass
import importlib
import importlib.resources as importlib_resources
import os
import pathlib
import tempfile
from contextlib import suppress
from importlib.resources.abc import Traversable
from pathlib import Path
from types import ModuleType

from dbx_core import imports


def path(
    *inputs,
    expanduser: bool = True,
    resolve: bool = True,
    absolute: bool = False,
    mkdir: bool = False,
    exists: bool = False,
) -> Path | None:
    """Best effort conversion of input to a ``Path`` with optional checks.
    When ``exists`` is true, returns None for non-existent paths.
    """
    try:
        result: pathlib.Path | None = None
        for input in inputs:
            if input is None:
                continue
            if not isinstance(input, Path):
                input = pathlib.Path(input)
            if expanduser:
                input = input.expanduser()
            if result is None:
                result = input
            else:
                if input.is_absolute():
                    result = None
                    break
                result = result / input
        if result is not None:
            if resolve:
                result = result.resolve()
            if absolute:
                result = result.absolute()
            if mkdir:
                result.mkdir(parents=True, exist_ok=True)
            if mkdir or (not exists or result.exists()):
                return result
    except Exception:
        pass
    return None


@functools.cache
def home(temp_dir_fallback: bool = True) -> Path:
    """Return the current user's home directory or a temp-backed fallback path."""
    home_path = path(Path.home(), exists=True)
    if home_path:
        return home_path
    try:
        username = getpass.getuser()
    except Exception:
        username = None
    home_path_parent = temp_dir() if temp_dir_fallback else Path(".").resolve()
    home_path = home_path_parent / ".home"
    if username:
        home_path = home_path / username
    home_path.mkdir(parents=True, exist_ok=True)
    return home_path


@functools.cache
def temp_dir() -> Path:
    """Return a usable temporary directory path with fallbacks across platforms."""
    temp_dir = path(tempfile.gettempdir(), exists=True)
    if not temp_dir:
        for fallback_dir in ["/tmp", "/temp"]:
            temp_dir = path(fallback_dir, exists=True)
            if temp_dir:
                break
    if not temp_dir:
        temp_dir = home(temp_fallback=False) / ".tmp"
        temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir


def module_path(
    module: str | os.PathLike[str] | ModuleType,
    *relative: str | os.PathLike[str],
    exists: bool = True,
) -> Path | None:
    """Resolve a path relative to a module from `__name__`, `__file__`, or module object.

    This helper supports package-resource lookup to remain usable when code is
    installed from wheels where direct filesystem paths may not be reliable.

    Args:
        module: Module identifier. Accepts module name (`__name__`), module
            object, or module file path (`__file__`).
        *relative: Relative path segments under the module directory.
        exists: If `True`, return `None` when the resolved file is missing.

    Returns:
        A resolved path to the requested resource when available, otherwise `None`.
    """
    relative_parts = [str(part) for part in relative if part is not None]
    if not relative_parts:
        return None

    module_file = imports.module_file_path(module)
    if module_file:
        direct_path = path(module_file.parent, *relative_parts, exists=exists)
        if direct_path:
            return direct_path
        if not exists:
            return path(module_file.parent, *relative_parts, exists=False)

    module_name = imports.module_name(module)
    if not module_name:
        return None

    resource_target = _resource_path(module_name, relative_parts)
    if resource_target:
        return resource_target
    return None if exists else path(*relative_parts, exists=False)


def _resource_path(module_name: str, relative_parts: list[str]) -> Path | None:
    with suppress(Exception):
        module = importlib.import_module(module_name)
        package_name = (
            module.__package__ or module_name.rpartition(".")[0] or module_name
        )
        resource = importlib_resources.files(package_name).joinpath(*relative_parts)
        if not resource.exists():
            return None
        if isinstance(resource, Path):
            return path(resource, exists=True)
        return _cache_resource(resource, package_name, relative_parts)
    return None


def _cache_resource(
    resource: Traversable, package_name: str, relative_parts: list[str]
) -> Path | None:
    """Copy a non-filesystem package resource to a stable temp path."""
    cache_root = temp_dir() / "dbx_core_resources" / package_name
    cache_path = cache_root.joinpath(*relative_parts)

    with importlib_resources.as_file(resource) as extracted:
        extracted_path = Path(extracted)
        if extracted_path.is_file():
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_bytes(extracted_path.read_bytes())
            return path(cache_path, exists=True)
        if extracted_path.is_dir():
            cache_path.mkdir(parents=True, exist_ok=True)
            return path(cache_path, exists=True)
    return None


if __name__ == "__main__":
    from lfp_logging import logs

    LOG = logs.logger()
    LOG.info(path(pathlib.Path("~/Desktop"), "/test.txt", absolute=True))
    LOG.info(path(pathlib.Path("~/Desktop"), "test.txt", absolute=True))
    LOG.info(path(pathlib.Path("~/Desktop"), "test.txt", 2, absolute=True))
    LOG.info(path("~/Desktop", "../test.txt", absolute=True))
    LOG.info(path("~/Desktop", "../test.txt", absolute=False))
    LOG.info(path("./test.txt", resolve=False, absolute=True))
    LOG.info(path("./test.txt", resolve=False, absolute=False))
    LOG.info(path("./test.txt", resolve=False, absolute=True))
    LOG.info(path("~/Desktop", absolute=True, exists=True))
    LOG.info(path("~/Desktop", absolute=True, exists=True, resolve=True))
