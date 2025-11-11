# reggie-bricks

A multi-module Python workspace managed with uv. This repository contains
multiple related projects organized as workspace members, each with its own
`pyproject.toml` and documentation.

## Overview

This repository breaks down different development needs at Databricks into
separate, focused sub-projects. Each sub-project addresses specific use cases
with minimal code and minimal dependency sharing, allowing projects to remain
lightweight and focused. By organizing functionality into discrete modules, we
avoid unnecessary dependencies and keep each project's scope clear and
maintainable.

## Repository Structure

This is a uv workspace. The root `pyproject.toml` declares the workspace, and
each member project has its own `pyproject.toml` that relies on the root for
shared `tool.uv.sources` configuration, allowing workspace dependencies to
resolve without duplicating configuration.

Each sub-project contains its own README with project-specific documentation.
See individual project directories for details.

## Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv)

## Local Setup

Synchronize the workspace to resolve all project dependencies:

```bash
uv sync --workspace
```

You can then execute any module script in place:

```bash
uv run --project <project-name> python -m <module>.<script>
```

Because all modules live in the same uv workspace, local changes in one project
are immediately visible to dependents without publishing wheels or editing
`PYTHONPATH`.

## Scripts

Workspace management scripts can be invoked using the `scripts.sh` wrapper:

```bash
./scripts.sh <script-name> [args...]
```

Run `./scripts.sh --help` for more information about available scripts and their
usage.

## Additional Notes

- Use `uv run --project <member>` for ad-hoc commands inside a specific module.
- See individual project READMEs for project-specific documentation and usage.
