"""Project name discovery helpers for modules, paths, and git repos."""

import functools
import pathlib
import subprocess
from os import PathLike
from typing import Any

_PYPROJECT_FILE_NAME = "pyproject.toml"


def root_dir(input: PathLike | str | None = None) -> pathlib.Path:
    if isinstance(input, str):
        return root_dir(pathlib.Path(input) if input else None)
    cwd = pathlib.Path.cwd().resolve()
    path = pathlib.Path(input).resolve() if input else None
    if path is None or path == cwd:
        return _root_dir_cwd(cwd)
    if result := _root_dir(path):
        return result
    raise ValueError(f"Could not determine root dir: {input}")


def _root_dir(path: pathlib.Path) -> pathlib.Path | None:
    if path.is_file():
        path = path.parent
    for proc_args in [
        ["uv", "workspace", "dir"],
        ["git", "rev-parse", "--show-toplevel"],
    ]:
        # noinspection PyBroadException
        try:
            proc = subprocess.run(
                proc_args,
                cwd=path,
                check=False,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
            if proc.returncode == 0:
                return pathlib.Path(proc.stdout.strip())
        except Exception:
            pass
    while path:
        if (path / _PYPROJECT_FILE_NAME).exists():
            return path
        path = path.parent
    raise None


@functools.lru_cache(maxsize=None)
def _root_dir_cwd(path: pathlib.Path) -> pathlib.Path:
    return _root_dir(path) or path


def root_pyproject(
    path: PathLike | str | None = None,
) -> tuple[pathlib.Path | None, dict[str, Any]]:
    pyproject = root_dir(path) / _PYPROJECT_FILE_NAME
    if pyproject.exists():
        try:
            import tomllib  # py3.11+
        except ModuleNotFoundError:
            import tomli as tomllib  # fallback
        with pyproject.open("rb") as f:
            return pyproject, tomllib.load(f)
    return None, {}


def root_project_name(path: PathLike | str | None = None) -> str | None:
    _, pdata = root_pyproject(path)
    if pdata:
        if project_name := pdata.get("project", {}).get("name", None):
            return project_name
    return root_dir(path).name
