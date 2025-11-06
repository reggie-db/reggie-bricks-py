#!/usr/bin/env python3
"""Synchronize requires-python specifiers across workspace member projects."""

import argparse
import logging
import sys

import utils

logging.basicConfig(level=logging.INFO, format="%(message)s")
LOG = logging.getLogger("python_version")


def run(min_version: str | None, max_version: str | None = None):
    projects = utils.workspace_pyprojects()
    if not min_version:
        for project in projects:
            project_min_version = project.requires_python_min
            if project_min_version is not None and (
                min_version is None or project_min_version < min_version
            ):
                min_version = project_min_version
    if not min_version:
        min_version = sys.version.split()[0]
    for project in projects:
        _process_project(project, min_version, max_version)


def _process_project(
    project: utils.PyProject, min_ver: str, max_ver: str | None
) -> None:
    """Load a project's pyproject.toml and update its requires-python field if needed."""
    with project.requires_python() as requires_python:
        requires_python.clear()
        requires_python.append(f">={min_ver}")
        if max_ver:
            requires_python.append(f"<{max_ver}")
    LOG.info(f"{project.project_path.parent.name} - min:{min_ver}, max:{max_ver}")


if __name__ == "__main__":
    """Entry point: parse args, enumerate members, and update each project."""
    parser = argparse.ArgumentParser(
        description="Update requires-python range for workspace members."
    )
    parser.add_argument("min_version", help="Minimum Python version, e.g. 3.9")
    parser.add_argument(
        "max_version", nargs="?", help="Optional maximum Python version, e.g. 4.0"
    )
    args = parser.parse_args()
    run(args.min_version, args.max_version)
