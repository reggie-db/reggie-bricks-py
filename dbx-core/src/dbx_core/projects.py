"""Project name discovery helpers for modules, paths, and git repos."""

import functools
import pathlib
import subprocess
from os import PathLike
from typing import Any

from dbx_core import strs

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
    return cwd


def _root_dir(path: pathlib.Path) -> pathlib.Path | None:
    if path.is_file():
        path = path.parent
    for proc_args in [
        ["uv", "workspace", "dir"],
        ["git", "rev-parse", "--show-toplevel"],
    ]:
        exit_code, stdout, _ = _run_command(proc_args, cwd=path)
        if exit_code == 0 and stdout:
            return pathlib.Path(stdout)
    while path:
        if (path / _PYPROJECT_FILE_NAME).exists():
            return path
        parent = path.parent
        if not parent or parent == path:
            break
        path = parent
    return None


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


def root_project_version(
    path: PathLike | str | None = None, git_fallback: bool = True
) -> str | None:
    _, pdata = root_pyproject(path)
    if pdata:
        if project_version := pdata.get("project", {}).get("version", None):
            return project_version
    rev = None
    if git_fallback:
        exit_code, stdout, _ = _run_command(["git", "rev-parse", "--short", "HEAD"])
        if exit_code == 0:
            rev = stdout
    version = "0.0.1"
    return f"{version}+g{rev}" if rev else version


def _run_command(
    proc_args: list[str], cwd: pathlib.Path | None = None
) -> tuple[int, str, str]:
    """Run a command and return ``(exit_code, stdout, stderr)``.

    The caller is responsible for deciding what exit codes mean.
    """
    # noinspection PyBroadException
    try:
        proc = subprocess.run(
            proc_args,
            cwd=cwd,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return proc.returncode, strs.trim(proc.stdout), strs.trim(proc.stderr)
    except Exception as exc:
        return 1, "", strs.trim(exc)
