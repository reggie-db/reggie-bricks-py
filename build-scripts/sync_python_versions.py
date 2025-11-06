#!/usr/bin/env python3
"""Synchronize requires-python specifiers across workspace member projects."""

import argparse
import logging
import sys

import utils

logging.basicConfig(level=logging.INFO, format="%(message)s")
LOG = logging.getLogger("python_version")


def run(min_version: str | None, max_version: str | None = None):
    pyprojects = list(utils.workspace_pyprojects())
    if not min_version:
        for pyproject in pyprojects:
            pyproject_min_version = pyproject.requires_python_min
            if pyproject_min_version is not None and (
                min_version is None or pyproject_min_version > min_version
            ):
                min_version = pyproject_min_version

    if not min_version:
        min_version = sys.version.split()[0]
    for pyproject in pyprojects:
        _process_project(pyproject, min_version, max_version)


def _process_project(
    project: utils.PyProject, min_version: str, max_version: str | None
) -> None:
    """Load a project's pyproject.toml and update its requires-python field if needed."""
    with project.requires_python() as requires_python:
        requires_python.clear()
        requires_python.append(f">={min_version}")
        if max_version:
            requires_python.append(f"<{max_version}")
    LOG.info(
        f"{project.pyproject_path.parent.name} - min:{min_version}, max:{max_version}"
    )


if __name__ == "__main__":
    """Entry point: parse args, enumerate members, and update each project."""
    parser = argparse.ArgumentParser(
        description="Update requires-python range for workspace members."
    )
    parser.add_argument(
        "min_version", nargs="?", help="Minimum Python version, e.g. 3.9"
    )
    parser.add_argument(
        "max_version", nargs="?", help="Optional maximum Python version, e.g. 4.0"
    )
    args = parser.parse_args()
    run(args.min_version, args.max_version)
