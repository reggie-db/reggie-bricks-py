# dbx-tools

Databricks specific utilities for Spark sessions, configuration management, catalog
access, and runtime detection. This module provides the core functionality needed to
work with Databricks workspaces, clusters, and notebooks.

## Overview

`dbx-tools` builds on `dbx-core` to provide Databricks specific functionality.
It handles Spark session creation, Databricks SDK configuration, catalog/schema
discovery, and runtime environment detection. This module is designed to minimize
boilerplate when working with Databricks.

## Features

### Client Management (`clients.py`)

Factory functions for creating Databricks clients:

* **WorkspaceClient**: Create `WorkspaceClient` instances for Databricks API access
* **Spark sessions**: Intelligent Spark session creation with automatic detection:
    * Prefers existing IPython injected sessions
    * Falls back to active local Spark sessions
    * Uses Databricks runtime sessions when available
    * Creates Databricks Connect sessions for local development

### Configuration (`configs.py`)

Databricks SDK configuration management:

* **Config caching**: Process wide configuration caching to avoid repeated initialization
* **Profile selection**: Automatic Databricks CLI profile discovery and selection
* **Multi source config values**: Unified configuration value resolution from:
    * Databricks widgets (notebook parameters)
    * Spark configuration
    * Environment variables
    * Databricks secrets (scoped by catalog or schema)
* **CLI integration**: Integration with Databricks CLI for authentication and profile
  management

### Catalog Utilities (`catalogs.py`)

Catalog and schema discovery:

* **Catalog or schema detection**: Automatically discover the active catalog and schema
  from Spark session or configuration
* **Fully qualified names**: Build fully qualified table references
* **Pipeline support**: Special handling for Databricks pipeline environments

### Runtime Detection (`runtimes.py`)

Environment and context detection:

* **Runtime version**: Detect Databricks runtime version
* **IPython integration**: Access IPython user namespace for notebook variables
* **DBUtils access**: Get DBUtils handle for current Spark session
* **Context assembly**: Build comprehensive runtime context from notebook and Spark
  sources
* **Environment detection**: Identify execution context:
    * Notebook vs. job vs. pipeline
    * Interactive vs. batch execution

### Spark Functions (`funcs.py`)

Custom Spark SQL functions for JSON processing:

* **JSON schema inference**: Infer schema from JSON string columns
* **JSON type detection**: Detect JSON value types (array, object, string, number,
  boolean, null)
* **JSON parsing**: Parse JSON with external schema lookup support

### Models (`models.py`)

Pydantic model extensions:

* **SchemaModel**: Base model with JSON schema generation utilities
* **Response format**: Generate OpenAI compatible response format schemas
* **Key exclusion**: Recursively remove keys from generated schemas

### Genie API (`genie.py`)

Databricks Genie API client for AI assistant interactions:

* **Service**: Client for creating conversations and sending messages to Genie
* **GenieResponse**: Wrapper for extracting queries and descriptions from responses
* **Streaming responses**: Poll for incremental updates as Genie processes requests
* **Query extraction**: Extract SQL queries and descriptions from Genie message attachments

## Usage

```python
from dbx_tools import clients, configs, catalogs, runtimes

# Get Spark session (automatically detects environment)
spark = clients.spark()

# Get workspace client
workspace = clients.workspace_client()

# Get configuration
config = configs.get()

# Discover catalog and schema
catalog_schema = catalogs.catalog_schema(spark)
print(f"Using {catalog_schema}")

# Check runtime context
if runtimes.is_notebook():
  print("Running in notebook")
elif runtimes.is_job():
  print("Running in job")
elif runtimes.is_pipeline():
  print("Running in pipeline")

# Get config value from multiple sources
value = configs.value(
  "my_setting",
  default="default_value",
  config_value_sources=[
    configs.ConfigValueSource.WIDGETS,
    configs.ConfigValueSource.OS_ENVIRON,
  ]
)

# Use Genie API for AI assistance
from dbx_tools import genie

service = genie.Service(clients.workspace_client(), "space-id")
conversation = service.create_conversation("Initial question")
for response in service.chat(conversation.conversation_id, "Follow-up question"):
  for query in response.queries():
    spark.sql(query).show()
```

## Dependencies

* `dbx-core` (workspace dependency)
* `databricks-sdk`
* `databricks-connect`
* `pydantic`

## Configuration Sources

Configuration values are resolved in order of precedence:

1. **WIDGETS**: Databricks notebook widgets
2. **SPARK_CONF**: Spark configuration (`spark.conf`)
3. **OS_ENVIRON**: Environment variables
4. **SECRETS**: Databricks secrets (scoped by catalog or schema)

## Environment Variables

* `DATABRICKS_RUNTIME_VERSION`: Set automatically in Databricks runtime environments
* `ENABLE_REPL_LOGGING`: Indicates interactive Databricks notebook session

