"""Path helpers for resolving, validating, and locating common directories."""

import functools
import getpass
import tempfile
from pathlib import Path

_CACHE_VERSION = 1


def path(
    input, resolve: bool = True, exists: bool = False, absolute: bool = False
) -> Path | None:
    """Best effort conversion of input to a ``Path`` with optional checks.
    When ``exists`` is true, returns None for non-existent paths.
    """
    if input is not None:
        try:
            if not isinstance(input, Path):
                input = Path(input)
            if resolve:
                input = input.resolve()
            if absolute:
                input = input.absolute()
            if not exists or input.exists():
                return input
        except Exception:
            pass
    return None


@functools.cache
def temp_dir() -> Path:
    """Return a usable temporary directory path with fallbacks across platforms."""
    temp_dir = path(tempfile.gettempdir(), exists=True)
    if not temp_dir:
        temp_dir = path("/tmp", exists=True)
    if not temp_dir:
        temp_dir = path("./tmp")
        if not temp_dir.exists:
            temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir


@functools.cache
def home() -> Path:
    """Return the current user's home directory or a temp-backed fallback path."""
    try:
        home_path = path(Path.home(), exists=True)
        if home_path:
            return home_path
    except Exception:
        pass
    try:
        username = getpass.getuser()
    except Exception:
        username = None
    if not username:
        username = "home"
    td = temp_dir()
    return td / username
