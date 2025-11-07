"""Path helpers for resolving, validating, and locating common directories."""

import functools
import getpass
import pathlib
import tempfile
from pathlib import Path


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


if __name__ == "__main__":
    print(path(pathlib.Path("~/Desktop"), "/test.txt", absolute=True))
    print(path(pathlib.Path("~/Desktop"), "test.txt", absolute=True))
    print(path(pathlib.Path("~/Desktop"), "test.txt", 2, absolute=True))
    print(path("~/Desktop", "../test.txt", absolute=True))
    print(path("~/Desktop", "../test.txt", absolute=False))
    print(path("./test.txt", resolve=False, absolute=True))
    print(path("./test.txt", resolve=False, absolute=False))
    print(path("./test.txt", resolve=False, absolute=True))
    print(path("~/Desktop", absolute=True, exists=True))
    print(path("~/Desktop", absolute=True, exists=True, resolve=True))
