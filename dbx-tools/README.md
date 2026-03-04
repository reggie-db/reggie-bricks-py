# dbx-tools

Databricks-focused utilities for client creation, runtime detection, catalog resolution, configuration lookup, and Genie interactions.

## What It Provides

- `clients.py`
  - Builds `WorkspaceClient` instances with product metadata in the user agent on the local fallback path.
  - Uses `databricks_tools_core.get_workspace_client` when available.
  - Falls back to local auth resolution for host/token and Databricks Apps OAuth env vars.
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

You can override order or exclude sources with `ConfigValueSource.without(...)`.

## Notes on Optional Integrations

- `databricks-tools-core` is optional. When installed, client creation delegates to it.
- Without notebook `dbutils`, secret lookup falls back to workspace API secret retrieval.

## Dependencies

- `dbx-core` (workspace dependency)
- `databricks-connect`
- `pydantic`
- `pyyaml`
- `python-dotenv`

