"""Catalog and schema discovery utilities for Databricks workspaces."""

import functools
import re
import uuid
from builtins import hasattr
from dataclasses import dataclass

from pyspark.sql import SparkSession

from dbx_tools import clients, configs, runtimes


@dataclass(frozen=True)
class CatalogSchema:
    """Fully qualified catalog/schema pair extracted from Spark metadata."""

    catalog: str
    schema: str

    def __str__(self) -> str:
        return f"{self.catalog}.{self.schema}"


@dataclass(frozen=True)
class CatalogSchemaTable(CatalogSchema):
    """Catalog/schema/table triple used for formatting fully qualified names."""

    table: str

    def __str__(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


@functools.cache
def _catalog_schema_config() -> CatalogSchema | None:
    """Attempt to resolve catalog and schema using configuration sources."""
    config_value_sources = configs.ConfigValueSource.without(
        configs.ConfigValueSource.SECRETS
    )
    catalog_name = configs.value(
        "catalog_name", None, config_value_sources=config_value_sources
    )
    if catalog_name:
        schema_name = configs.value(
            "schema_name", None, config_value_sources=config_value_sources
        )
        if schema_name:
            return CatalogSchema(catalog_name, schema_name)
    if runtimes.is_pipeline():
        catalog_schemas: set[CatalogSchema] = set()
        try:
            # Reference a nonexistent table to surface a fully qualified path in the error message
            clients.spark().sql(
                f"SELECT * FROM table_{uuid.uuid4().hex} LIMIT 1"
            ).count()
        except Exception as e:
            msg = str(e)
            matches = re.findall(r"`([^`]+)`\.`([^`]+)`\.`([^`]+)`", msg)
            for c, s, _ in matches:
                if c and s:
                    catalog_schemas.add(CatalogSchema(c, s))
        if len(catalog_schemas) == 1:
            return next(iter(catalog_schemas))
    return None


def catalog_schema(spark: SparkSession | None = None) -> CatalogSchema | None:
    """Derive the active catalog/schema, preferring configuration hints first."""
    catalog_schema_config = _catalog_schema_config()
    if catalog_schema_config:
        return catalog_schema_config
    if not spark:
        spark = clients.spark()
    if hasattr(spark, "catalog"):
        spark_catalog = spark.catalog
        if (
            spark_catalog
            and hasattr(spark_catalog, "currentCatalog")
            and hasattr(spark_catalog, "currentDatabase")
        ):
            # Use high level Spark API when available
            current_catalog = spark_catalog.currentCatalog()
            current_schema = spark_catalog.currentDatabase()
            if current_catalog and current_schema:
                return CatalogSchema(current_catalog, current_schema)
    catalog_schema_row = (
        (spark or clients.spark())
        .sql("SELECT current_catalog() AS catalog, current_schema() AS schema")
        .first()
    )
    # Fallback to SQL when the API path did not return values
    if catalog_schema_row.catalog and catalog_schema_row.schema:
        return CatalogSchema(catalog_schema_row.catalog, catalog_schema_row.schema)
    return None


def catalog_schema_table(
    table: str, spark: SparkSession = None
) -> CatalogSchemaTable | None:
    """Return a fully qualified table reference for the provided ``table`` name."""
    if table:
        cs = catalog_schema(spark)
        if cs:
            return CatalogSchemaTable(catalog=cs.catalog, schema=cs.schema, table=table)
    return None


def catalog(spark: SparkSession = None) -> str | None:
    """Return only the catalog component from :func:`catalog_schema`."""
    cs = catalog_schema(spark)
    return cs.catalog if cs else None


def schema(spark: SparkSession = None) -> str | None:
    """Return only the schema component from :func:`catalog_schema`."""
    cs = catalog_schema(spark)
    return cs.schema if cs else None
