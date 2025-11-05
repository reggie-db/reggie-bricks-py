#!/usr/bin/env python3
import argparse
import logging
import pathlib
from typing import Optional

import tomli_w
import tomllib
import utils
from packaging.specifiers import SpecifierSet
from packaging.version import InvalidVersion, Version

logging.basicConfig(level=logging.INFO, format="%(message)s")
LOG = logging.getLogger("python_version")

PY_PROJECT_FILE_NAME = "pyproject.toml"


def load_workspace_members(root: pathlib.Path) -> list[str]:
    pyproject = tomllib.loads((root / PY_PROJECT_FILE_NAME).read_text())
    return (
        pyproject.get("tool", {}).get("uv", {}).get("workspace", {}).get("members", [])
    )


def candidate_projects(root: pathlib.Path, member_pattern: str) -> list[pathlib.Path]:
    paths: list[pathlib.Path] = []
    for p in root.glob(member_pattern):
        if not p.is_dir():
            continue
        pj = p / PY_PROJECT_FILE_NAME
        if pj.exists():
            paths.append(p)
        else:
            for child in p.iterdir():
                if child.is_dir() and (child / PY_PROJECT_FILE_NAME).exists():
                    paths.append(child)
    return paths


def update_requires_python(current: str, new_min: str, new_max: str | None) -> str:
    """Update requires-python spec using packaging library."""
    try:
        current_spec = SpecifierSet(current or "")
    except Exception:
        current_spec = SpecifierSet("")

    new_spec_parts = [f">={new_min}"]
    if new_max:
        new_spec_parts.append(f"<{new_max}")
    else:
        # keep current max only if it's lower than min
        for s in current_spec:
            if s.operator in ("<", "<="):
                try:
                    if Version(s.version) < Version(new_min):
                        new_spec_parts.append(str(s))
                except InvalidVersion:
                    pass

    return ",".join(new_spec_parts)


def process_project(
    py_path: pathlib.Path, min_ver: str, max_ver: str | None
) -> None:
    data = tomllib.loads(py_path.read_text())
    proj = data.setdefault("project", {})
    current = proj.get("requires-python", "")
    updated = update_requires_python(current, min_ver, max_ver)
    if updated != current:
        proj["requires-python"] = updated
        LOG.info(f"{py_path.parent.name}: {current or '<none>'} -> {updated}")
        py_path.write_text(tomli_w.dumps(data))
    else:
        LOG.info(f"{py_path.parent.name}: unchanged ({current or '<none>'})")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Update requires-python range for workspace members."
    )
    parser.add_argument("min_version", help="Minimum Python version, e.g. 3.9")
    parser.add_argument(
        "max_version", nargs="?", help="Optional maximum Python version, e.g. 4.0"
    )
    args = parser.parse_args()

    root = utils.repo_root()
    members = load_workspace_members(root)
    if not members:
        raise SystemExit("No workspace members found under [tool.uv.workspace].")

    projects: list[pathlib.Path] = []
    seen = set()
    for m in members:
        for proj in candidate_projects(root, m):
            rp = proj.resolve()
            if rp not in seen:
                seen.add(rp)
                projects.append(proj)

    for proj in projects:
        py_path = proj / PY_PROJECT_FILE_NAME
        process_project(py_path, args.min_version, args.max_version)


if __name__ == "__main__":
    main()
