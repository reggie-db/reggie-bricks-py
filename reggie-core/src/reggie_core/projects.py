"""Project name discovery helpers for modules, paths, and git repos."""

import functools
import importlib
import subprocess
import tomllib
from pathlib import Path
from types import ModuleType

from reggie_core import paths


def name(input=None, default: str = None, git_origin: bool = True) -> str:
    """
    Resolve a project name from a module, path, or string.

    Order of resolution:
      1) If input is a module, try its distribution name via importlib.metadata.
      2) If git_origin is True, try the current repo origin's last path segment.
      3) Walk parent directories from input path to find pyproject.toml project.name.
      4) Fall back to the provided default, else raise ValueError.

    Args:
        input: Module, path-like, or string used as a hint. If None, defers to _project_name_default.
        default: Fallback name if discovery fails.
        git_origin: Whether to consult `git remote get-url origin`.

    Returns:
        The resolved project name as a string.

    Raises:
        ValueError: When the name cannot be determined and no default is provided.
    """
    if input is None and default is None:
        return _name_default()
    name = None
    if input is not None:
        if isinstance(input, ModuleType):
            name = input.__package__ or input.__name__
        name = str(name).split(".")[0]
    if name:
        try:
            dist = importlib.metadata.distribution(name)
            project_name = dist.metadata["Name"] if dist else None
            if project_name:
                return project_name
        except Exception:
            pass
    if isinstance(input, ModuleType):
        path = paths.path(input.__file__, exists=True)
    else:
        path = paths.path(input, exists=True)
    if path and git_origin:
        if project_name := _remote_origin_name(path):
            return project_name

    project_name = default
    while path:
        path = path.parent
        path_project_name = None
        if pyproject := paths.path(path / "pyproject.toml", exists=True):
            try:
                if data := tomllib.loads(pyproject.read_text()):
                    path_project_name = data.get("project", {}).get("name", None)
            except Exception:
                pass

        if not path_project_name and project_name is not None:
            break
        elif path_project_name:
            project_name = path_project_name
    if project_name:
        return project_name
    raise ValueError(f"Could not determine project name for {input}")


def _remote_origin_name(path=None) -> str | None:
    path = paths.path(path, exists=False)
    if path and path.is_file():
        path = path.parent
    try:
        proc = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            cwd=path or None,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        return None
    remote_origin_url = proc.stdout
    if remote_origin_url:
        remote_origin_url = remote_origin_url.strip()
    if remote_origin_url:
        if project_name := remote_origin_url.split("/")[-1].split(".")[0]:
            return project_name


@functools.cache
def _name_default() -> str:
    """
    Best-effort default name. Tries file based resolution first then module name,
    finally returns a constant if both fail.
    """
    pn = name(__file__, "")
    if not pn:
        pn = name(__name__, "")
    if not pn:
        pn = name(Path.cwd(), "")
    return pn or "reggie-bricks"
