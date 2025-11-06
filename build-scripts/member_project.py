#!/usr/bin/env python3

"""Create or update a workspace member project with proper dependencies and sources."""

import pathlib
import re
import sys

import sync_python_versions
import utils


def die(msg: str, code: int = 1) -> None:
    """Exit the script with a message and non-zero status."""
    print(msg, file=sys.stderr)
    raise SystemExit(code)


if len(sys.argv) < 2:
    die(f"Usage: {pathlib.Path(sys.argv[0]).name} <project-name> [member-dep ...]")

raw = sys.argv[1]
member_deps = list(dict.fromkeys(sys.argv[2:]))  # dedupe, preserve order

# tokenize name: split on non alnum and lowercase
parts = [p.lower() for p in re.split(r"[^A-Za-z0-9]+", raw) if p]
if not parts:
    die("Project name must contain alphanumeric characters")

name_dash = "-".join(parts)  # project dir and project.name
name_us = "_".join(parts)  # package dir

root = utils.repo_root()
root_pyproject = utils.PyProject(root)

if member_deps:
    root_members = root_pyproject.members
    for dep in member_deps:
        if dep not in root_members:
            die(f"Member project does not exist: {dep}")

proj_dir = root / name_dash
proj_pyproject = utils.PyProject(proj_dir)
pkg_dir = proj_dir / "src" / name_us
pkg_dir.mkdir(parents=True, exist_ok=True)
pkg_init = pkg_dir / "__init__.py"
if not pkg_init.is_file():
    pkg_init.write_text("", encoding="utf-8")


pyproject_content = {
    "build-system": {
        "requires": ["uv_build>=0.8.23,<0.9.0"],
        "build-backend": "uv_build",
    },
    "project": {
        "name": name_dash,
        "version": "0.0.1",
        "requires-python": ">=3",
        "dependencies": [],
    },
}


with proj_pyproject.pyproject() as pyproject:
    build_system = pyproject.setdefault("build-system", {})
    build_system.setdefault("requires", ["uv_build>=0.8.23,<0.9.0"])
    build_system.setdefault("build-backend", "uv_build")

    project = pyproject.setdefault("project", {})
    project.setdefault("name", name_dash)
    project.setdefault("version", "0.0.1")
    project.setdefault("requires-python", ">=3")
    dependencies = project.setdefault("dependencies", [])

    if member_deps:
        file_dependencies = [
            f"{dep} @ file://${{PROJECT_ROOT}}/../{dep}" for dep in member_deps
        ]
        for dep in file_dependencies:
            if dep not in dependencies:
                dependencies.append(dep)

        sources = (
            pyproject.setdefault("tool", {})
            .setdefault("uv", {})
            .setdefault("sources", {})
        )
        for dep in member_deps:
            src = sources.get(dep)
            if not isinstance(src, dict) or not src.get("workspace", False):
                sources[dep] = {"workspace": True}

    optional_dependencies = project.setdefault("optional-dependencies", {})
    optional_dependencies.setdefault("dev", []).append("pytest")


with root_pyproject.pyproject() as root_pyproject:
    members = (
        root_pyproject.setdefault("tool", {})
        .setdefault("uv", {})
        .setdefault("workspace", {})
        .setdefault("members", [])
    )
    if name_dash not in members:
        members.append(name_dash)

sync_python_versions.run()

print(f"Project directory: {proj_dir}")
print(f"[Package module:    {name_us}")
print(f"Workspace member:  {name_dash}")
if member_deps:
    print("Ensured member dependencies and workspace sources for:")
    for d in member_deps:
        print(f"     - {d}")
