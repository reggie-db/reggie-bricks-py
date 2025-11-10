import argparse
import os
import pathlib
import re
import shutil
import subprocess
import sys
from copy import deepcopy
from typing import Callable, Mapping

import tomlkit
from benedict import benedict

from scripts import projects
from scripts.projects import Project

_DEFAULT_VERSION = "0.0.1"


def sync():
    member_projects = list(projects.root().members())
    sync_build_system(*member_projects)
    sync_version(*member_projects)
    sync_tool_member_project(*member_projects)
    sync_member_dependencies(*member_projects)
    for member_project in member_projects:
        _persist(member_project)


def sync_build_system(*pyprojects: Project):
    root = projects.root()

    def _set(p: Project):
        key = "build-system"
        data = root.pyproject.get(key, None)
        if data:
            p.pyproject.merge({key: deepcopy(data)}, overwrite=True)

    _sync(_set, *pyprojects)


def sync_version(*pyprojects: Project, version: str | None = None):
    if not version:
        version = _git_version() or _DEFAULT_VERSION

    def _set(p: Project):
        data = {"project": {"version": version}}
        p.pyproject.merge(data, overwrite=True)

    _sync(_set, *pyprojects)


def sync_tool_member_project(*pyprojects: Project):
    root = projects.root()

    def _set(p: Project):
        data = root.pyproject.get("tool.member-project", None)
        if data:
            p.pyproject.merge(deepcopy(data), overwrite=True)

    _sync(_set, *pyprojects)


def sync_member_dependencies(*pyprojects: Project):
    # reggie-core @ file://${PROJECT_ROOT}/../reggie-core

    root = projects.root()
    member_project_names = set(p.name for p in root.members())

    def parse_dep_name(dep: str) -> str | None:
        m = re.match(r"^\s*([\w\-\.\[\]]+)\s*@\s*file://", dep)
        return m.group(1) if m else dep

    def _set(p: Project):
        doc = p.pyproject
        deps = doc.get("project.dependencies", [])
        member_deps = []
        for i in range(len(deps)):
            dep = parse_dep_name(deps[i])
            if dep not in member_project_names:
                continue
            file_dep = dep + " @ file://${PROJECT_ROOT}/../" + dep
            member_deps.append(dep)
            deps[i] = file_dep
        sources_path = "tool.uv.sources"
        sources = doc.get(sources_path, None)
        if isinstance(sources, Mapping):
            del_deps = []
            for k, v in sources.items():
                if k not in member_deps and v.workspace == True:
                    del_deps.append(k)

            for dep in del_deps:
                del sources[dep]
        if member_deps:
            data = {}
            for member_dep in member_deps:
                # tool.uv.sources.[name]
                data.setdefault("tool", {}).setdefault("uv", {}).setdefault("sources", {}).setdefault(member_dep, {})[
                    "workspace"] = True
            p.pyproject.merge(data)

    _sync(_set, *pyprojects)


def clean_build_artifacts():
    root = projects.root_pyproject().file.parent
    root_venv = root / ".venv"
    excludes = [
        lambda path: path.name == ".venv" and path.parent == root,
        lambda path: projects.scripts_pyproject().file in path.parents,
    ]
    matchers = [
        lambda path: path.name == ".venv",
        lambda path: path.name == "__pycache__" and path.parent != root_venv,
        lambda path: path.name.endswith(".egg-info"),
    ]
    for root_path, dir_names, _ in os.walk(root):
        path = pathlib.Path(root_path)
        if any(f(path) for f in excludes):
            dir_names[:] = []
            continue
        if any(f(path) for f in matchers):
            dir_names[:] = []
            print(f"Deleting directory:{path}")
            shutil.rmtree(path)


def _sync(pyproject_fn: Callable[[Project], None], *pyprojects: Project):
    if len(pyprojects) == 0:
        pyprojects = list(projects.root().members())
    for p in pyprojects:
        pyproject_fn(p)


def _persist(project: Project, prune: bool = True):
    file = project.pyproject_file
    doc = project.pyproject
    if prune and isinstance(doc, benedict):
        doc.clean(strings=False)
    text = tomlkit.dumps(doc)
    current_text = file.read_text() if file.exists() else None
    if text != current_text:
        file.write_text(text)
        print(f"Project updated - {project}")


def _git_version() -> str:
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
    return None


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
    # main()
    # sync_member_dependencies()
    sync()
