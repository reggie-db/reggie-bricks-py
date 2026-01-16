"""
Workspace management CLI tool for synchronizing and managing Python projects.

This module provides a Typer-based CLI application for managing a workspace of Python projects.
It supports synchronizing project configurations (versions, build systems, dependencies),
creating new member projects, and cleaning build artifacts across the workspace.
"""

import functools
import inspect
import os
import pathlib
import re
import shutil
import subprocess
from copy import deepcopy
from typing import Annotated, Any, Callable, Iterable, Mapping

import click
import tomlkit
import typer
from benedict.dicts import benedict
from dbx_core import logs

from scripts import projects
from scripts.projects import Project

# Default version string used when git version cannot be determined
_DEFAULT_VERSION = "0.0.1"


LOG = logs.logger(__file__)


def _sync_projects_option_callback(ctx: typer.Context, sync_projects: Iterable[Any]):
    """
    Callback for processing sync project options from CLI.

    Converts project identifiers to Project objects and stores them in context metadata
    for later use by the sync result callback.

    Args:
        ctx: Typer context object containing command metadata
        sync_projects: Iterable of project identifiers (names or Project objects)

    Returns:
        List of Project objects
    """
    sync_projects = list(_projects(sync_projects))
    ctx.meta["sync_projects"] = sync_projects
    return sync_projects


@click.pass_context
def _sync_result_callback(ctx: typer.Context, _):
    """
    Result callback for sync commands that persists project changes.

    This callback is invoked after sync commands complete to save any modifications
    made to project pyproject.toml files.

    Args:
        ctx: Typer context object containing command metadata
        _: Unused parameter (required by Typer callback signature)
    """
    key = "sync_projects"
    if key in ctx.meta:
        _persist_projects(ctx.meta[key])


_SYNC_PROJECTS_OPTION = Annotated[
    list[str],
    typer.Option(
        "-p",
        "--project",
        callback=_sync_projects_option_callback,
        help="Optional list of project names or identifiers to sync",
    ),
]

app = typer.Typer()

sync = typer.Typer(result_callback=_sync_result_callback)
app.add_typer(sync, name="sync")

create = typer.Typer()
app.add_typer(create, name="create")

clean = typer.Typer()
app.add_typer(clean, name="clean")


@sync.command(
    name="all",
    help="Synchronize all project configurations across specified projects. Executes all registered sync commands (except sync_all itself) for the given projects. If no projects are specified, operates on all workspace member projects.",
)
def sync_all(sync_projects: _SYNC_PROJECTS_OPTION = None):
    _sync_all(sync_projects)


def _sync_all(sync_projects: Iterable[Any] = None):
    """
    Internal function to execute all sync commands for given projects.

    Iterates through all registered sync commands and executes them (excluding sync_all)
    for the specified projects.

    Args:
        sync_projects: Optional iterable of project identifiers to sync
    """
    projs = list(_projects(sync_projects))
    for cmd in sync.registered_commands:
        callback = cmd.callback
        # Skip sync_all to avoid infinite recursion
        if "sync_all" != getattr(callback, "__name__", None):
            sig = inspect.signature(callback)
            callback(projs) if len(sig.parameters) >= 1 else callback()


@sync.command(
    name="build-system",
    help="Synchronize build-system configuration from root project to member projects. Copies the build-system section from the root project's pyproject.toml to all specified member projects. Includes the scripts project by default.",
)
def sync_build_system(sync_projects: _SYNC_PROJECTS_OPTION = None):
    def _set(p: Project):
        key = "build-system"
        data = projects.root().pyproject.get(key, None)
        if data:
            # Use deepcopy to avoid mutating the root project's data
            p.pyproject.merge({key: deepcopy(data)}, overwrite=True)

    _update_projects(_set, sync_projects, include_scripts=True)


@sync.command(
    name="version",
    help="Synchronize version numbers across specified projects. Sets the project version in pyproject.toml for all specified projects. If no version is provided, attempts to derive version from git commit hash, falling back to the default version if git is unavailable.",
)
def sync_version(
    sync_projects: _SYNC_PROJECTS_OPTION = None,
    version: Annotated[
        str, typer.Argument(help="Version string (e.g., '1.2.3' or '0.0.1+gabc123')")
    ] = None,
):
    if not version:
        version = _git_version() or _DEFAULT_VERSION

    def _set(p: Project):
        data = {"project": {"version": version}}
        p.pyproject.merge(data, overwrite=True)

    _update_projects(_set, sync_projects)


@sync.command(
    name="member-project-tool",
    help="Synchronize tool.member-project configuration from root to member projects. Copies the tool.member-project section from the root project's pyproject.toml to all specified member projects.",
)
def sync_member_project_tool(sync_projects: _SYNC_PROJECTS_OPTION = None):
    def _set(p: Project):
        data = projects.root().pyproject.get("tool.member-project", None)
        if data:
            p.pyproject.merge(deepcopy(data), overwrite=True)

    _update_projects(_set, sync_projects)


@sync.command(
    name="member-project-dependencies",
    help="Synchronize member project dependencies to use workspace file references. Updates project dependencies to reference workspace member projects using file:// URLs with ${PROJECT_ROOT} placeholders. Also configures tool.uv.sources to mark these dependencies as workspace members and removes stale workspace source entries for dependencies that are no longer member projects. Example: 'dbx-core' -> 'dbx-core @ file://${PROJECT_ROOT}/../dbx-core'",
)
def sync_member_project_dependencies(sync_projects: _SYNC_PROJECTS_OPTION = None):
    # Get list of all member project names for dependency matching
    member_project_names = list(p.name for p in projects.root().members())

    def parse_dep_name(dep: str) -> str | None:
        """
        Extract the base package name from a dependency string.

        Handles both plain package names and file:// URL dependencies.
        Returns the package name without the file:// URL prefix if present.

        Args:
            dep: Dependency string (e.g., "package" or "package @ file://...")

        Returns:
            Base package name or the original string if no file:// URL found
        """
        m = re.match(r"^\s*([\w\-\.\[\]]+)\s*@\s*file://", dep)
        return m.group(1) if m else dep

    def _set(p: Project):
        doc = p.pyproject
        deps = doc.get("project.dependencies", [])
        member_deps = []
        # Update dependencies to use file:// URLs for member projects
        for i in range(len(deps)):
            dep = parse_dep_name(deps[i])
            if dep not in member_project_names:
                continue
            # Transform to file:// URL format: "package @ file://${PROJECT_ROOT}/../package"
            file_dep = dep + " @ file://${PROJECT_ROOT}/../" + dep
            member_deps.append(dep)
            deps[i] = file_dep
        # Update tool.uv.sources to mark member dependencies as workspace members
        sources_path = "tool.uv.sources"
        sources = doc.get(sources_path, None)
        if isinstance(sources, Mapping):
            # Remove stale workspace entries for dependencies that are no longer members
            del_deps = []
            for k, v in sources.items():
                if k not in member_deps and (v.get("workspace", None) is True):
                    del_deps.append(k)

            for dep in del_deps:
                del sources[dep]
        # Add workspace source entries for all member dependencies
        if member_deps:
            data = {}
            for member_dep in member_deps:
                # Build nested structure: tool.uv.sources.[name].workspace = True
                data.setdefault("tool", {}).setdefault("uv", {}).setdefault(
                    "sources", {}
                ).setdefault(member_dep, {})["workspace"] = True
            p.pyproject.merge(data)

    _update_projects(_set, sync_projects)


@sync.command(
    name="ruff",
    help="Run ruff on git tracked python fils",
)
def sync_ruff():
    ruff_exec = _exec("ruff")
    if ruff_exec:
        git_files = _git_files()
        if git_files:
            py_files = [str(f) for f in git_files if f.name.endswith(".py")]
            subprocess.run(["ruff", "format", *py_files], check=True)


@create.callback(
    invoke_without_command=True,
    help="Create a new member project in the workspace. Creates a new Python project directory with a pyproject.toml file and initial package structure. The project will be synchronized with workspace defaults (build-system, version, etc.) after creation.",
)
def create_member(
    name: Annotated[
        str,
        typer.Argument(
            help="Name of the project to create (will be used as directory name)"
        ),
    ],
    path: Annotated[
        pathlib.Path,
        typer.Option(
            dir_okay=True,
            file_okay=False,
            help="Optional parent directory path (must be within workspace root). If not provided, creates project in workspace root.",
        ),
    ] = None,
    project_dependencies: Annotated[
        list[str],
        typer.Option(
            "-pd",
            "--project-dependency",
            help="Optional list of project dependency names to include in the new project's dependencies",
        ),
    ] = None,
):
    if path:
        path = path.resolve()
        if not path.is_relative_to(projects.root_dir()):
            raise ValueError(f"Invalid path:{path}")
    else:
        path = projects.root_dir()
    path = path / name
    # Ensure we're working with the pyproject.toml file path
    if path.name != projects.PYPROJECT_FILE_NAME:
        path = path / projects.PYPROJECT_FILE_NAME
    if path.is_file():
        raise ValueError(f"Project exists path:{path}")
    project_dir = path.parent
    project_dir.mkdir(parents=True, exist_ok=True)
    project_name = project_dir.name
    LOG.info(f"Creating member project - name:{project_name} dir:{project_dir}")
    # Initialize pyproject.toml with basic structure
    pyproject_toml = benedict(tomlkit.document(), keyattr_dynamic=True)
    pyproject_toml["build-system"] = {}
    pyproject_toml["project"] = {
        "name": project_name,
        "version": "0",
        "requires-python": ">=3.6",
    }
    # Add project dependencies if specified
    if project_dependencies:
        dependencies = tomlkit.array()
        dependencies.multiline(True)
        for dep in project_dependencies:
            dep_dir = projects.dir(dep)
            dep_project = Project(dep_dir)
            dependencies.append(dep_project.name)
        pyproject_toml["project"]["dependencies"] = dependencies
    path.write_text(tomlkit.dumps(pyproject_toml))
    # Create package directory structure: src/{package_name}/__init__.py
    package_dir = project_dir / "src" / project_name.replace("-", "_")
    package_dir.mkdir(parents=True, exist_ok=True)
    (package_dir / "__init__.py").touch()
    # Synchronize with workspace defaults and persist changes
    proj = Project(project_dir)
    _sync_all([proj])
    _persist_projects([proj])


@clean.command(
    name="build-artifacts",
    help="Remove build artifacts from the workspace. Deletes common Python build artifacts including .venv directories (except root workspace .venv), __pycache__ directories (except in root workspace .venv), and .egg-info directories. Excludes the scripts directory and root workspace .venv from cleanup.",
)
def clean_build_artifacts():
    root = projects.root_dir()
    root_venv = root / ".venv"
    # Directories to exclude from cleanup
    excludes = [
        lambda p: p.name == ".venv" and p.parent == root,
        lambda p: projects.scripts_dir() in p.parents,
    ]
    # Patterns for directories to delete
    matchers = [
        lambda p: p.name == ".venv",
        lambda p: p.name == "__pycache__" and p.parent != root_venv,
        lambda p: p.name.endswith(".egg-info"),
    ]
    for root_path, dir_names, _ in os.walk(root):
        path = pathlib.Path(root_path)
        # Skip excluded directories and their children
        if any(f(path) for f in excludes):
            dir_names[:] = []
            continue
        # Delete matching directories
        if any(f(path) for f in matchers):
            dir_names[:] = []
            LOG.info(f"Deleting directory:{path}")
            shutil.rmtree(path)


@functools.lru_cache(maxsize=None)
def _exec(name: str) -> pathlib.Path | None:
    path = shutil.which(name)
    if path:
        return pathlib.Path(path)
    LOG.warning(f"Executable not found: {name}")
    return None


@functools.cache
def _git_files() -> list[pathlib.Path] | None:
    git_exec = _exec("git")
    if git_exec:
        try:
            result = subprocess.run(
                [str(git_exec), "ls-files"],
                capture_output=True,
                text=True,
                check=True,
            )
            return [pathlib.Path(f) for f in result.stdout.splitlines() if f]
        except Exception:
            pass
    return None


def _git_version() -> str | None:
    """
    Build a workspace version string from git commit hash.

    Constructs a version string in the format <default_version>+g<short_rev> using
    the current git commit hash. Returns None if git is unavailable or the command
    fails.

    Returns:
        Version string like "0.0.1+g767bd46" or None if git is unavailable
    """
    git_exec = _exec("git")
    if git_exec:
        try:
            rev = subprocess.check_output(
                [str(git_exec), "rev-parse", "--short", "HEAD"],
                cwd=pathlib.Path(__file__).resolve().parents[1],
                text=True,
            ).strip()
            if rev:
                return f"{_DEFAULT_VERSION}+g{rev}"
        except Exception:
            pass
    return None


def _update_projects(
    pyproject_fn: Callable[[Project], None],
    projs: Iterable[Any] | None,
    include_scripts: bool = False,
):
    """
    Apply a function to update pyproject.toml for multiple projects.

    Iterates through projects and applies the provided function to each project's
    pyproject configuration. By default, excludes the scripts project unless
    include_scripts is True.

    Args:
        pyproject_fn: Function that takes a Project and modifies its pyproject attribute
        projs: Optional iterable of project identifiers to update.
               If None, updates all workspace member projects.
        include_scripts: If True, includes the scripts project in updates
    """
    for proj in _projects(projs):
        if not include_scripts and proj.is_scripts:
            continue
        pyproject_fn(proj)


def _persist_projects(projs: Iterable[Any] = None, prune: bool = True):
    """
    Save pyproject.toml changes to disk for specified projects.

    Writes the pyproject configuration back to the pyproject.toml file for each
    project, but only if the content has changed. Optionally prunes empty values
    from the configuration before saving.

    Args:
        projs: Optional iterable of project identifiers to persist.
               If None, persists all workspace member projects.
        prune: If True, removes empty values from configuration before saving
    """
    for proj in _projects(projs):
        file = proj.pyproject_file
        doc = proj.pyproject
        # Remove empty values to keep configuration clean
        if prune and isinstance(doc, benedict):
            doc.clean(strings=False)
        text = tomlkit.dumps(doc)
        current_text = file.read_text() if file.exists() else None
        # Only write if content has changed
        if text != current_text:
            file.write_text(text)
            LOG.info(f"Project updated:{file}")


def _projects(projs: Iterable[Any] = None) -> Iterable[Project]:
    """
    Convert project identifiers to Project objects.

    Takes an iterable of project identifiers (names, paths, or Project objects)
    and yields Project objects. If no projects are specified, yields all workspace
    member projects.

    Args:
        projs: Optional iterable of project identifiers (names, paths, or Project objects)

    Yields:
        Project objects for each valid project identifier

    Raises:
        ValueError: If a project identifier cannot be resolved to a valid project
    """
    if not projs:
        projs = projects.root().members()
    for proj in projs:
        if not isinstance(proj, Project):
            LOG.debug(f"Resolving project identifier: {proj}")
            project_dir = projects.dir(proj)
            LOG.debug(f"Resolved project directory: {project_dir.absolute()}")
            if not project_dir:
                raise ValueError(f"Project {proj} not found - sync_projects: {projs}")
            proj = Project(project_dir)
        yield proj


@app.callback(
    invoke_without_command=True,
    help="Workspace management CLI tool for synchronizing and managing Python projects.",
)
def main():
    # sync()
    pass


if __name__ == "__main__":
    app()
