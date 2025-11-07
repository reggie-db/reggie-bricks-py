import argparse
import os
import pathlib
import platform
import shutil
import subprocess
import sys
from typing import Any

from scripts import projects

from scripts.projects import root_pyproject, PyProject

_DEFAULT_VERSION = "0.0.1"


def update_versions(version: str | None = None):
    if not version:
        version = _version()
    _set_member_pyproject_values("project", "version", version)


def update_requires_python(specifier: str | None = None):
    root_pyp = root_pyproject()
    if not specifier:
        specifier = root_pyp.data.get("tools.settings.requires-python", None)
    if not specifier:
        version = platform.python_version_tuple()
        specifier = f">={version[0]}.{version[1]}"
    _set_member_pyproject_values("project", "requires-python", specifier, False)


def _set_member_pyproject_values(path: str, key: str, value: Any, create: bool = True):
    root_pyp = root_pyproject()
    for p in [root_pyp] + list(root_pyp.members()):
        _set_pyproject_value(p, path, key, value, create)


def _set_pyproject_value(pyproject: PyProject, path: str, key: str, value: Any, create: bool = True):
    with pyproject.edit() as data:
        if node := data.get(path, None) if path else data:
            if value:
                if create or key in node:
                    node[key] = value
            elif key in node:
                del node[key]


def clean_build_artifacts():
    root = projects.root_pyproject().pyproject.parent
    root_venv = root / ".venv"
    excludes = [
        lambda path: path.name == ".venv" and path.parent == root,
        lambda path: projects.scripts_pyproject().pyproject in path.parents,
    ]
    matchers = [
        lambda path: path.name == ".venv",
        lambda path: path.name == "__pycache__" and path.parent != root_venv,
        lambda path: path.name.endswith(".egg-info"),
    ]
    for dirpath, dirnames, _ in os.walk(root):
        path = pathlib.Path(dirpath)
        if any(f(path) for f in excludes):
            dirnames[:] = []
            continue
        if any(f(path) for f in matchers):
            dirnames[:] = []
            print(f"Deleting directory:{path}")
            shutil.rmtree(path)


def _version() -> str:
    """Build a workspace version string of the form 0.0.1+g<rev> when git is available."""
    try:
        rev = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=pathlib.Path(__file__).resolve().parents[1],
            text=True,
        ).strip()
        if rev:
            return f"{_DEFAULT_VERSION}+g{rev}"
    except Exception:
        pass
    return _DEFAULT_VERSION


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", help="Method name to run")
    parser.add_argument("args", nargs=argparse.REMAINDER)
    parsed = parser.parse_args()

    cmd = parsed.command
    func = globals().get(cmd) if cmd and not cmd.startswith("_") else None
    if not callable(func):
        print(f"Unknown command: {cmd}", file=sys.stderr)
        sys.exit(1)

    func(*parsed.args)


if __name__ == "__main__":
    main()
