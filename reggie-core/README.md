# reggie-core

Lightweight shared utilities intended to be reused by higher-level modules without
carrying heavy dependencies. This module provides foundational functionality for
logging, object serialization, path handling, and common parsing operations.

## Overview

`reggie-core` is designed as a minimal dependency base layer. It contains only
essential utilities that other projects in the workspace can depend on without
introducing unnecessary bloat. The module focuses on common operations needed
across Databricks development workflows.

## Features

### Logging (`logs.py`)

Automatic logging configuration with intelligent server vs. interactive detection:

- **Auto-configuration**: Automatically configures logging handlers based on runtime
  environment (server vs. interactive terminal)
- **Smart routing**: Routes INFO logs via `print()` in interactive environments for
  better notebook/UI integration
- **Color support**: ANSI color coding by log level when supported
- **Server detection**: Automatically detects server environments (CI, containers,
  Databricks jobs) and adjusts formatting accordingly

Environment variables:
- `LOGGING_SERVER`: Force server mode even if TTY exists
- `LOGGING_PRINT`: Enable print routing for INFO logs on interactive terminals
- `LOGGING_AUTO_CONFIG`: Enable/disable automatic logging configuration

### Object Serialization (`objects.py`)

Utilities for converting objects to dictionaries and JSON:

- **Dataclass support**: Automatic conversion of dataclasses to dictionaries
- **Property extraction**: Optionally include `@property` values in serialization
- **JSON encoding**: Custom JSON encoders with ISO date handling
- **Hashing**: SHA-256 hashing with optional pickle support and key sorting

### Path Utilities (`paths.py`)

Cross-platform path resolution and directory discovery:

- **Path resolution**: Best-effort conversion of inputs to `Path` objects with
  validation
- **Home directory**: User home directory resolution with temp fallback
- **Temp directory**: Reliable temporary directory access across platforms

### Input Handling (`inputs.py`)

Interactive user input utilities:

- **TTY detection**: Determine if a process can safely prompt for user input
- **Choice selection**: Numbered menu system for user selection in interactive
  environments

### Parsing (`parsers.py`)

Common value coercion utilities:

- **Boolean parsing**: Flexible boolean conversion from strings, integers, and other
  types with configurable defaults

### Project Discovery (`projects.py`)

Project name resolution from various sources:

- **Module detection**: Extract project names from Python modules
- **Git integration**: Derive project names from git remote URLs
- **pyproject.toml**: Read project names from `pyproject.toml` files

### String Utilities (`strs.py`)

Tokenization helpers for names and identifiers:

- **Tokenization**: Split strings on non-alphanumeric characters and camelCase
  boundaries
- **Normalization**: Lowercase and normalize token fragments

## Usage

```python
from reggie_core import logs, objects, paths

# Automatic logging configuration
log = logs.logger(__name__)
log.info("Message")

# Object serialization
data = objects.dump(my_object, member_properties=True)
json_str = objects.to_json(data)

# Path resolution
config_path = paths.path("~/config", "settings.json", exists=True)
```

## Dependencies

- `sitecustomize-entrypoints` - For automatic logging configuration via entry points

## Entry Points

- `logs_auto_config`: Automatically configures logging when the module is imported
  (can be disabled via `LOGGING_AUTO_CONFIG` environment variable)

