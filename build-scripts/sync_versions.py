#!/usr/bin/env python3

"""Synchronize project versions across workspace members based on git revision."""

import logging
import pathlib
import subprocess

import tomli_w
import tomllib
import utils

logging.basicConfig(level=logging.INFO, format="%(message)s")
LOG = logging.getLogger(__name__)

DEFAULT_VERSION = "0.0.1"
VERSION_KEY = "version"


def version() -> str:
    """Build a workspace version string of the form 0.0.1+g<rev> when git is available."""
    try:
        rev = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=pathlib.Path(__file__).resolve().parents[1],
            text=True,
        ).strip()
        if rev:
            return f"{DEFAULT_VERSION}+g{rev}"
    except Exception:
        pass
    return DEFAULT_VERSION


def main():
    """Entry point: compute version and write it into each member's pyproject."""
    root = utils.repo_root()
    pyproject_version = version()
    pyproject_root = tomllib.loads((root / utils.PY_PROJECT_FILE_NAME).read_text())
    members = (
        pyproject_root.get("tool", {})
        .get("uv", {})
        .get("workspace", {})
        .get("members", [])
    )
    if not members:
        raise SystemExit("No workspace members found under [tool.uv.workspace].")

    projects = utils.workspace_projects(root, members)

    for proj in projects:
        py_path = proj / utils.PY_PROJECT_FILE_NAME
        data = tomllib.loads(py_path.read_text())

        proj_tbl = data.setdefault("project", {})
        current = proj_tbl.get(VERSION_KEY, "<none>")

        # If version was dynamic, remove it so a static version applies
        dynamic_key = "dynamic"
        dyn = proj_tbl.get(dynamic_key)
        if isinstance(dyn, list) and VERSION_KEY in dyn:
            proj_tbl[dynamic_key] = [x for x in dyn if x != VERSION_KEY]
            if not proj_tbl[dynamic_key]:
                proj_tbl.pop(dynamic_key)

        proj_tbl[VERSION_KEY] = pyproject_version
        LOG.info(f"{proj.name}: {current} -> {pyproject_version}")

        py_path.write_text(tomli_w.dumps(data))


if __name__ == "__main__":
    main()
