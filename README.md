# reggie-bricks

Multimodule Python workspace managed with `uv` and `uv_build`.

## Workspace Members

- `dbx-ai`: PydanticAI + Databricks model serving helpers.
- `dbx-caddy`: Caddy binary install and process helpers.
- `dbx-concurio`: Disk cache and executable install helpers.
- `dbx-core`: Shared base utilities (objects, paths, parsing, project discovery).
- `dbx-cv`: Computer-vision utilities for RTSP frame workflows.
- `dbx-ip`: IP lookup helpers backed by `ipwho.is`.
- `dbx-openapi`: OpenAPI code generation utilities used by workspace tooling.
- `dbx-postgres`: Local and remote Postgres helpers on top of SQLAlchemy.
- `dbx-reflex`: Reusable Reflex components and state/event helpers.
- `dbx-tools`: Databricks-focused runtime, config, warehouse, Genie, and experiment helpers.
- `demo-iot`: FastAPI and Kafka based IoT demo application.

## Prerequisites

- Python `>=3.11,<3.13`
- [uv](https://github.com/astral-sh/uv)

## Local Setup

```bash
uv sync --workspace
```

Run package modules directly with workspace resolution:

```bash
uv run --project <project-name> python -m <module>.<script>
```

## Notes

- Each package has its own `pyproject.toml`.
- Workspace package references are managed with `tool.uv.sources` and local file dependencies.
- See package-level READMEs for package-specific examples and behavior.
