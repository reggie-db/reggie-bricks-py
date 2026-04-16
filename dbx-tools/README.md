# dbx-tools

Databricks-focused utilities for client creation, runtime detection, catalog resolution, configuration lookup, and Genie interactions.

## What It Provides

- `clients.py`
  - Builds `WorkspaceClient` instances with product metadata in the user agent.
  - Supports host/token overrides and Databricks Apps OAuth env vars.
  - Selects a preferred SQL warehouse by serverless, size, then name preference.
  - Creates Spark sessions from notebook/runtime context and Databricks Connect when needed.
- `configs.py`
  - Returns a runtime-aware Databricks SDK `Config`.
  - Resolves config values from ordered sources (widgets, Spark conf, secrets, env vars).
  - Falls back to `databricks bundle validate --output json` variable values when available.
- `catalogs.py`
  - Resolves active catalog/schema from explicit config, runtime inference, or Spark SQL.
  - Builds fully qualified table names with `CatalogSchemaTable`.
- `runtimes.py`
  - Detects Databricks runtime and app environment metadata.
  - Exposes IPython namespace/context helpers.
  - Resolves notebook/job/pipeline execution context.
- `experiments.py`
  - Resolves MLflow experiments by id/name/path with optional create-on-miss behavior.
  - Handles Databricks App creator permission repair for auto-created experiments.
- `warehouses.py`
  - Ranks warehouses, executes SQL statements, and parses table metadata.
- `genie.py`
  - Helpers for interacting with Databricks Genie conversations and responses.
- `models.py`
  - Model serving and endpoint utility helpers.
- `funcs.py`
  - Spark JSON helper functions for model-friendly payload handling.

## Usage

```python
from dbx_tools import catalogs, clients, configs, runtimes

workspace = clients.workspace_client()

# Avoid forcing Databricks Connect when caller only wants optional Spark access.
spark = clients.spark(connect=False)

active = catalogs.catalog_schema(spark)
setting = configs.value("my_setting", default_value="default")

if runtimes.is_pipeline():
    pass
```

## Config Value Resolution Order

`configs.value()` checks sources in this order by default:

1. `ConfigValueSource.WIDGETS`
2. `ConfigValueSource.SPARK_CONF`
3. `ConfigValueSource.SECRETS`
4. `ConfigValueSource.OS_ENVIRON`

When bundle context is available, `configs.value()` also attempts Databricks Bundle variable values from `databricks bundle validate --output json`.

You can override order or exclude sources with `ConfigValueSource.without(...)`.

## Notes on Optional Integrations

- Without notebook `dbutils`, secret lookup falls back to workspace API secret retrieval.

## Dependencies

- `dbx-core` (workspace dependency)
- `databricks-connect`
- `pydantic`
- `pyyaml`
- `python-dotenv`

