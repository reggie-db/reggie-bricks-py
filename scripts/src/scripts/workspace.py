import os
import pathlib
import re
import shutil
import subprocess
from copy import deepcopy
from typing import Callable, Mapping, Annotated, Any, Iterable

import click
import tomlkit
import typer
from benedict import benedict

from scripts import projects
from scripts.projects import Project

_DEFAULT_VERSION = "0.0.1"


def _sync_projects_option_callback(ctx: typer.Context, sync_projects: Iterable[Any]):
    sync_projects = list(_projects(sync_projects))
    ctx.meta["sync_projects"] = sync_projects
    return sync_projects


@click.pass_context
def _sync_result_callback(ctx: typer.Context, _):
    key = "sync_projects"
    if key in ctx.meta:
        _persist_projects(ctx.meta[key])


_SYNC_PROJECTS_OPTION = Annotated[
    list[str], typer.Option("-p", "--project", callback=_sync_projects_option_callback)]

app = typer.Typer()

sync = typer.Typer(result_callback=_sync_result_callback)
app.add_typer(sync, name="sync")

create = typer.Typer()
app.add_typer(create, name="create")

clean = typer.Typer()
app.add_typer(clean, name="clean")


@sync.command(name="all")
def sync_all(sync_projects: _SYNC_PROJECTS_OPTION = None):
    _sync_all(sync_projects)


def _sync_all(sync_projects: Iterable[Any] = None):
    projs = list(_projects(sync_projects))
    for cmd in sync.registered_commands:
        callback = cmd.callback
        if "sync_all" != getattr(callback, "__name__", None):
            callback(projs)


@sync.command(name="build-system")
def sync_build_system(sync_projects: _SYNC_PROJECTS_OPTION = None):
    def _set(p: Project):
        key = "build-system"
        data = projects.root().pyproject.get(key, None)
        if data:
            p.pyproject.merge({key: deepcopy(data)}, overwrite=True)

    _update_projects(_set, sync_projects, include_scripts=True)


@sync.command(name="version")
def sync_version(sync_projects: _SYNC_PROJECTS_OPTION = None, version: Annotated[str, typer.Argument()] = None):
    if not version:
        version = _git_version() or _DEFAULT_VERSION

    def _set(p: Project):
        data = {"project": {"version": version}}
        p.pyproject.merge(data, overwrite=True)

    _update_projects(_set, sync_projects)


@sync.command(name="member-project-tool")
def sync_member_project_tool(sync_projects: _SYNC_PROJECTS_OPTION = None):
    def _set(p: Project):
        data = projects.root().pyproject.get("tool.member-project", None)
        if data:
            p.pyproject.merge(deepcopy(data), overwrite=True)

    _update_projects(_set, sync_projects)


@sync.command(name="member-project-dependencies")
def sync_member_project_dependencies(sync_projects: _SYNC_PROJECTS_OPTION = None):
    # reggie-core @ file://${PROJECT_ROOT}/../reggie-core
    member_project_names = list(p.name for p in projects.root().members())

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
                if k not in member_deps and (v.get("workspace", None) is True):
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

    _update_projects(_set, sync_projects)


@create.callback(invoke_without_command=True)
def create_member(name: str, path: Annotated[
    pathlib.Path,
    typer.Option(dir_okay=True, file_okay=False)] = None,
                  project_dependencies: Annotated[
                      list[str],
                      typer.Option("-pd", "--project-dependency")] = None, ):
    if path:
        path = path.resolve()
        if not path.is_relative_to(projects.root_dir()):
            raise ValueError(f"Invalid path:{path}")
    else:
        path = projects.root_dir()
    path = path / name
    if path.name != projects.PYPROJECT_FILE_NAME:
        path = path / projects.PYPROJECT_FILE_NAME
    if path.is_file():
        raise ValueError(f"Project exists path:{path}")
    project_dir = path.parent
    project_dir.mkdir(parents=True, exist_ok=True)
    project_name = project_dir.name
    print(f"Creating member project - name:{project_name} dir:{project_dir}")
    pyproject_toml = benedict(tomlkit.document(), keyattr_dynamic=True)
    pyproject_toml["build-system"] = {}
    pyproject_toml["project"] = {
        "name": project_name,
        "version": "0",
        "requires-python": ">=3.6"
    }
    if project_dependencies:
        dependencies = tomlkit.array()
        dependencies.multiline(True)
        for dep in project_dependencies:
            dep_dir = projects.dir(dep)
            dep_project = Project(dep_dir)
            dependencies.append(dep_project.name)
        pyproject_toml["project"]["dependencies"] = dependencies
    path.write_text(tomlkit.dumps(pyproject_toml))
    package_dir = project_dir / "src" / project_name.replace("-", "_")
    package_dir.mkdir(parents=True, exist_ok=True)
    (package_dir / "__init__.py").touch()
    proj = Project(project_dir)
    _sync_all([proj])
    _persist_projects([proj])


@clean.command(name="build-artifacts")
def clean_build_artifacts():
    root = projects.root_dir()
    root_venv = root / ".venv"
    excludes = [
        lambda p: p.name == ".venv" and p.parent == root,
        lambda p: projects.scripts_dir() in p.parents,
    ]
    matchers = [
        lambda p: p.name == ".venv",
        lambda p: p.name == "__pycache__" and p.parent != root_venv,
        lambda p: p.name.endswith(".egg-info"),
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


def _git_version() -> str | None:
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


def _update_projects(pyproject_fn: Callable[[Project], None], projs: Iterable[Any] | None, include_scripts: bool = False):
    for proj in _projects(projs):
        if not include_scripts and proj.is_scripts:
            continue
        pyproject_fn(proj)


def _persist_projects(projs: Iterable[Any] = None, prune: bool = True):
    for proj in _projects(projs):
        file = proj.pyproject_file
        doc = proj.pyproject
        if prune and isinstance(doc, benedict):
            doc.clean(strings=False)
        text = tomlkit.dumps(doc)
        current_text = file.read_text() if file.exists() else None
        if text != current_text:
            file.write_text(text)
            print(f"Project updated:{file}")


def _projects(projs: Iterable[Any] = None) -> Iterable[Project]:
    if not projs:
        projs = projects.root().members()
    for proj in projs:
        if not isinstance(proj, Project):
            print(proj)
            project_dir = projects.dir(proj)
            print(project_dir.absolute())
            if not project_dir:
                raise ValueError(f"Project {proj} not found - sync_projects: {projs}")
            proj = Project(project_dir)
        yield proj


@app.callback(invoke_without_command=True)
def main():
    # sync()
    pass


if __name__ == "__main__":
    app()
