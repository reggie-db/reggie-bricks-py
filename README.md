# reggie-bricks

A multimodule Python workspace managed with uv. This repository contains
multiple related projects organized as workspace members, each with its own
`pyproject.toml` and documentation.

## Overview

This repository breaks down different development needs into
separate, focused subprojects. Each subproject addresses specific use cases
with minimal code and minimal dependency sharing, allowing projects to remain
lightweight and focused. By organizing functionality into discrete modules, we
avoid unnecessary dependencies and keep each project's scope clear and
maintainable.

## Workspace Members

The following projects are included in this workspace:

### Core Modules

* `reggie-core`: Lightweight shared utilities for logging, object serialization, path handling, and common parsing operations.
* `dbx-tools`: Databricks specific utilities for Spark sessions, configuration management, catalog access, and Genie API integration.
* `dbx-concurio`: Execution and caching logic using `diskcache` and file locking.

### Applications and Orchestration

* `dbx-app-runner`: Tools for orchestrating applications including Caddy, Docker, Conda, and Git management.
* `dbx-reflex`: Web applications and components built with the Reflex framework.
* `dbx-cv`: Computer vision utilities using OpenCV and image hashing.

### Examples and Demos

* `demo-iot`: An IoT demonstration project featuring FastAPI, Kafka integration, and reactive programming.

## Repository Structure

This is a uv workspace. The root `pyproject.toml` declares the workspace, and
each member project has its own `pyproject.toml` that relies on the root for
shared `tool.uv.sources` configuration, allowing workspace dependencies to
resolve without duplicating configuration.

Each subproject contains its own README with project specific documentation.
See individual project directories for details.

## Prerequisites

* Python 3.12+
* [uv](https://github.com/astral-sh/uv)

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

## Additional Notes

* Use `uv run --project <member>` for ad-hoc commands inside a specific module.
* See individual project READMEs for project specific documentation and usage.
