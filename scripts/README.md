# scripts

Internal workspace management and utility scripts for the reggie-bricks project.

## Overview

The `scripts` module provides tools for maintaining the workspace, including project synchronization, code generation, and repository management.

## Features

### Workspace Management (`workspace.py`)

CLI tool for workspace operations:

* **Sync**: Synchronize project configurations (versions, dependencies, build systems) across the workspace.
* **Create**: Scaffold new workspace member projects with standard structure.
* **Clean**: Remove build artifacts and temporary files.

### Code Generation (`openapi.py`)

OpenAPI based code generation:

* **Sync Generated Code**: Synchronize generated FastAPI code with change detection.
* **Template Support**: Use custom Jinja2 templates for code generation.

### Project Utilities (`projects.py`)

Shared logic for project discovery:

* **Root Discovery**: Locate the workspace root directory.
* **Project Models**: Object oriented representation of workspace projects and their `pyproject.toml` files.

## Usage

Workspace scripts are typically invoked via the root `scripts.sh` wrapper:

```bash
./scripts.sh workspace --help
```

To synchronize all projects:

```bash
./scripts.sh workspace sync all
```

## Dependencies

* `reggie-core` (workspace dependency)
* `typer`
* `tomlkit`
* `benedict`
* `fastapi-code-generator`

