# reggie-app-runner

Tools for orchestrating and running applications with support for Caddy, Docker, Conda, and Git management. This module provides a unified way to configure and launch applications across different environments.

## Overview

`reggie-app-runner` handles application lifecycle management by providing abstractions for environment setup (Conda), container runtimes (Docker/udocker), reverse proxies (Caddy), and source code management (Git). It uses `dynaconf` for flexible configuration.

## Features

### App Runner (`app_runner.py`)

Unified application configuration and execution:

* **AppRunnerConfig**: Configuration management using `dynaconf` with support for environment variable overrides.
* **Multi source support**: Run applications from local paths, GitHub repositories, or Docker images.
* **Dependency resolution**: Automatic setup of Conda environments and Pip dependencies.

### Container Runtime (`docker.py`)

Abstraction over Docker and podman:

* **Automatic detection**: Prefers `docker` or `podman` if available.
* **Fallback support**: Falls back to `udocker` within a managed Conda environment if no native runtime is found.
* **Image management**: Pull images and compute stable image hashes.

### Reverse Proxy (`caddy.py`)

Managed Caddy instances for application routing:

* **Automatic installation**: Manages Caddy installation via Conda.
* **Dynamic configuration**: Run Caddy with JSON, Caddyfile, or dictionary based configurations.
* **Log integration**: Intelligent log level mapping from Caddy JSON logs to standard Python logging.

### Conda Management (`conda.py`)

Interactions with Conda and Micromamba:

* **Environment management**: Create and update Conda environments.
* **Command execution**: Run commands within specific Conda environments.

### Git Integration (`git.py`)

Source code management:

* **Repository handling**: Clone and manage Git repositories.
* **Commit resolution**: Retrieve remote commit hashes for stable application versions.

## Usage

```python
from reggie_app_runner import app_runner

# Read configurations from environment and settings files
configs = list(app_runner.read_configs())

for config in configs:
    print(f"Running app: {config.name}")
    # Application execution logic...
```

## Dependencies

* `reggie-core` (workspace dependency)
* `reggie-tools` (workspace dependency)
* `dynaconf`
* `sh`
* `psutil`
* `requirements-parser`
* `giturlparse`

