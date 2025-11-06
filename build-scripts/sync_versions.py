#!/usr/bin/env python3

"""Synchronize project versions across workspace members based on git revision."""

import logging
import pathlib
import subprocess

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
    pyproject_version = version()
    for pyproject in utils.workspace_pyprojects():
        print(f"pyproject:{pyproject.pyproject_path}")
        with pyproject.project() as project:
            print(project)
            # If version was dynamic, remove it so a static version applies
            dynamic_key = "dynamic"
            dyn = project.get(dynamic_key)
            if isinstance(dyn, list) and VERSION_KEY in dyn:
                project[dynamic_key] = [x for x in dyn if x != VERSION_KEY]
                if not project[dynamic_key]:
                    project.pop(dynamic_key)

            project[VERSION_KEY] = pyproject_version
            LOG.info(
                f"{pyproject.pyproject_path.parent.name} version: {pyproject_version}"
            )


if __name__ == "__main__":
    main()
