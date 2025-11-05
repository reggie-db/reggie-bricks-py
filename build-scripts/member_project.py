#!/usr/bin/env python3
"""Create or update a workspace member project with proper dependencies and sources."""

import pathlib
import re
import sys

import tomli_w
import tomllib

PYPROJECT_FILE_NAME = "pyproject.toml"


def die(msg: str, code: int = 1) -> None:
    """Exit the script with a message and non-zero status."""
    print(msg, file=sys.stderr)
    raise SystemExit(code)


def load_toml(path: pathlib.Path) -> dict:
    """Load TOML content or exit if the file does not exist."""
    if not path.exists():
        die(f"Missing TOML: {path}")
    return tomllib.loads(path.read_text(encoding="utf-8"))


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

root = pathlib.Path.cwd()
proj_dir = root / name_dash
proj_py_path = proj_dir / PYPROJECT_FILE_NAME

# ensure member projects exist and are valid
for dep in member_deps:
    dep_dir = root / dep
    dep_py = dep_dir / PYPROJECT_FILE_NAME
    if not dep_dir.exists():
        die(f"Member project directory does not exist: {dep_dir}")
    if not dep_py.exists():
        die(f"Member project pyproject.toml not found: {dep_py}")

# create structure if missing, otherwise continue and log
if proj_dir.exists():
    print(f"[info] Project exists, will update: {proj_dir}")
    if not proj_py_path.exists():
        die(f"Existing project missing {PYPROJECT_FILE_NAME}: {proj_py_path}")
else:
    # create minimal structure
    pkg_dir = proj_dir / "src" / name_us
    pkg_dir.mkdir(parents=True, exist_ok=False)
    (pkg_dir / "__init__.py").write_text("", encoding="utf-8")

# load or initialize project pyproject data
if proj_py_path.exists():
    py_content = load_toml(proj_py_path)
else:
    py_content = {
        "build-system": {
            "requires": ["uv_build>=0.8.23,<0.9.0"],
            "build-backend": "uv_build",
        },
        "project": {
            "name": name_dash,
            "version": "0.0.1",
            "requires-python": ">=3.12",
            "dependencies": [],
        },
    }

# enforce required keys
py_content.setdefault(
    "build-system",
    {
        "requires": ["uv_build>=0.8.23,<0.9.0"],
        "build-backend": "uv_build",
    },
)
project_tbl = py_content.setdefault("project", {})
project_tbl.setdefault("name", name_dash)
project_tbl.setdefault("version", "0.0.1")
project_tbl.setdefault("requires-python", ">=3.12")
deps_list = project_tbl.setdefault("dependencies", [])

# merge dependencies of form "dep @ file://${PROJECT_ROOT}/../dep"
want_deps = [f"{dep} @ file://${{PROJECT_ROOT}}/../{dep}" for dep in member_deps]
existing = set(deps_list)
for d in want_deps:
    if d not in existing:
        deps_list.append(d)
        existing.add(d)

# add uv workspace sources for each dep
tool_tbl = py_content.setdefault("tool", {})
uv_tbl = tool_tbl.setdefault("uv", {})
sources_tbl = uv_tbl.setdefault("sources", {})
for dep in member_deps:
    src = sources_tbl.get(dep)
    if not isinstance(src, dict) or not src.get("workspace", False):
        sources_tbl[dep] = {"workspace": True}

# write project pyproject.toml
proj_py_path.write_text(tomli_w.dumps(py_content), encoding="utf-8")

# update root workspace members
root_py_path = root / PYPROJECT_FILE_NAME
if not root_py_path.exists():
    die(f"Root {PYPROJECT_FILE_NAME} not found at {root_py_path}")

root_data = load_toml(root_py_path)
tool_root = root_data.setdefault("tool", {})
uv_root = tool_root.setdefault("uv", {})
ws = uv_root.setdefault("workspace", {})
members = ws.setdefault("members", [])

if name_dash not in members:
    members.append(name_dash)

root_py_path.write_text(tomli_w.dumps(root_data), encoding="utf-8")

print(f"Project directory: {proj_dir}")
print(f"[Package module:    {name_us}")
print(f"Workspace member:  {name_dash}")
if member_deps:
    print("Ensured member dependencies and workspace sources for:")
    for d in member_deps:
        print(f"     - {d}")
