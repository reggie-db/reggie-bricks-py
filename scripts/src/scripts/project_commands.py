import argparse
import os
import pathlib
import platform
import re
import shutil
import subprocess
import sys
from typing import Any

from scripts import projects

_DEFAULT_VERSION = "0.0.1"


def sync():
    sync_build_system()
    sync_version()
    sync_requires_python()
    sync_member_dependencies()


def sync_build_system():
    root_pyp = projects.root_pyproject()
    key = "build-system"
    build_system = root_pyp.data[key]
    _set_member_pyproject_values(None, key, build_system)


def sync_version(version: str | None = None):
    if not version:
        version = _version()
    _set_member_pyproject_values("project", "version", version)


def sync_requires_python(specifier: str | None = None):
    root_pyp = projects.root_pyproject()
    if not specifier:
        specifier = root_pyp.data.get("tools.settings.requires-python", None)
    if not specifier:
        version = platform.python_version_tuple()
        specifier = f">={version[0]}.{version[1]}"
    _set_member_pyproject_values("project", "requires-python", specifier, False)


def sync_member_dependencies(specifier: str | None = None):
    # reggie-core @ file://${PROJECT_ROOT}/../reggie-core

    def parse_dep_name(dep: str) -> str | None:
        m = re.match(r"^\s*([\w\-\.\[\]]+)\s*@\s*file://", dep)
        return m.group(1) if m else dep

    root_pyp = projects.root_pyproject()
    member_project_names = set(p.name for p in root_pyp.members())
    for p in root_pyp.members():
        with p.edit() as data:
            deps = data.get("project.dependencies", None)
            member_deps = []
            for i in range(len(deps) if deps else 0):
                dep = parse_dep_name(deps[i])
                if dep not in member_project_names:
                    continue
                file_dep = dep + " @ file://${PROJECT_ROOT}/../" + dep
                member_deps.append(dep)
                deps[i] = file_dep
            for member_dep in member_deps:
                # tool.uv.sources.scripts
                data[f"tool.uv.sources.{member_dep}.workspace"] = True


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


def _set_member_pyproject_values(path: str | None, key: str, value: Any, create: bool = True):
    root_pyp = projects.root_pyproject()
    for p in [root_pyp] + list(root_pyp.members()):
        with p.edit() as data:
            if node := data.get(path, None) if path else data:
                if value:
                    if create or key in node:
                        node[key] = value
                elif key in node:
                    del node[key]


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
    # sync_member_dependencies()
