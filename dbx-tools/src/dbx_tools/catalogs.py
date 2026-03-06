"""Catalog and schema discovery utilities for Databricks workspaces."""

import re
import uuid
from dataclasses import dataclass
from typing import Any, Iterable, TypeAlias

from lfp_types import T as _T
from pyspark.sql import SparkSession

from dbx_tools import clients, configs, runtimes

_UNSET = object()

CatalogSchemaLike: TypeAlias = str | Iterable[str] | "CatalogSchema"


@dataclass(frozen=True)
class CatalogSchema:
    """Fully qualified catalog/schema pair extracted from Spark metadata."""

    catalog: str
    schema: str

    def __post_init__(self):
        if not self.catalog:
            raise ValueError("Catalog cannot be empty")
        if not self.schema:
            raise ValueError("Schema cannot be empty")

    def __str__(self) -> str:
        return f"{self.catalog}.{self.schema}"

    @classmethod
    def of(
        cls, value: CatalogSchemaLike, default: "CatalogSchema" = _UNSET
    ) -> "CatalogSchema":
        return _of(cls, 2, value, default)


CatalogSchemaTableLike: TypeAlias = str | Iterable[str] | "CatalogSchemaTable"


@dataclass(frozen=True)
class CatalogSchemaTable(CatalogSchema):
    """Catalog/schema/table triple used for formatting fully qualified names."""

    table: str

    def __post_init__(self):
        super().__post_init__()
        if not self.table:
            raise ValueError("Table cannot be empty")

    def __str__(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"

    @classmethod
    def of(
        cls,
        value: CatalogSchemaTableLike,
        default: "CatalogSchemaTable" = _UNSET,
    ) -> "CatalogSchemaTable":
        return _of(cls, 3, value, default)


def _of(
    cls: type[_T], min_segments: int, value: Any, default: _T | None = _UNSET
) -> _T | None:
    if isinstance(value, cls):
        return value

    def _parts(raw: Any) -> Iterable[str]:
        """Yield flattened name segments across nested iterables."""
        if isinstance(raw, str):
            if not raw:
                raise ValueError("Empty segment")
            # Split all segments so extra dotted parts can be rejected.
            for segment in raw.split("."):
                if not segment:
                    raise ValueError("Empty segment")
                yield segment
            return
        if isinstance(raw, Iterable):
            for item in raw:
                yield from _parts(item)
            return
        if raw is None:
            raise ValueError("Empty segment")
        yield from _parts(str(raw))

    try:
        result = [segment for segment in _parts(value)]
    except ValueError:
        result = []

    # Reject over-qualified names instead of truncating.
    if min_segments <= len(result) <= 3:
        return cls(*result[0:min_segments])
    if default is not _UNSET:
        return default
    raise ValueError(f"Invalid {cls.__name__}: {value}")


def _catalog_schema_config() -> CatalogSchema | None:
    """Attempt to resolve catalog and schema from config and pipeline context."""
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
        if spark := clients.spark(connect=False):
            try:
                # Reference a non-existent table to surface an FQN in the error message.
                spark.sql(f"SELECT * FROM table_{uuid.uuid4().hex} LIMIT 1").count()
            except Exception as exc:
                msg = str(exc)
                matches = re.findall(r"`([^`]+)`\.`([^`]+)`\.`([^`]+)`", msg)
                for c, s, _ in matches:
                    if c and s:
                        catalog_schemas.add(CatalogSchema(c, s))
        if len(catalog_schemas) == 1:
            return next(iter(catalog_schemas))
    return None


def catalog_schema(spark: SparkSession | None = None) -> CatalogSchema | None:
    """Derive the active catalog/schema, preferring configuration hints first.

    Args:
        spark: Optional Spark session to use for runtime lookup.

    Returns:
        A ``CatalogSchema`` when it can be determined, otherwise ``None``.
    """
    catalog_schema_config = _catalog_schema_config()
    if catalog_schema_config:
        return catalog_schema_config
    if not spark:
        spark = clients.spark(connect=False)
    if spark:
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
        catalog_schema_row = spark.sql(
            "SELECT current_catalog() AS catalog, current_schema() AS schema"
        ).first()
        # Fallback to SQL when the API path did not return values
        if catalog_schema_row.catalog and catalog_schema_row.schema:
            return CatalogSchema(catalog_schema_row.catalog, catalog_schema_row.schema)
    return None


def catalog_schema_table(
    spark: SparkSession | None = None, table: str | None = None
) -> CatalogSchemaTable | None:
    """Return a fully qualified table reference for the provided table name.

    Args:
        table: Unqualified table name.
        spark: Optional Spark session used to infer catalog and schema.

    Returns:
        A ``CatalogSchemaTable`` when catalog/schema can be resolved, otherwise ``None``.
    """
    if table:
        cs = catalog_schema(spark)
        if cs:
            return CatalogSchemaTable(catalog=cs.catalog, schema=cs.schema, table=table)
    return None


def catalog(spark: SparkSession | None = None) -> str | None:
    """Return only the catalog component from :func:`catalog_schema`.

    Args:
        spark: Optional Spark session used to infer catalog/schema context.

    Returns:
        Catalog name when available, otherwise ``None``.
    """
    cs = catalog_schema(spark)
    return cs.catalog if cs else None


def schema(spark: SparkSession | None = None) -> str | None:
    """Return only the schema component from :func:`catalog_schema`.

    Args:
        spark: Optional Spark session used to infer catalog/schema context.

    Returns:
        Schema name when available, otherwise ``None``.
    """
    cs = catalog_schema(spark)
    return cs.schema if cs else None


if __name__ == "__main__":
    print(CatalogSchema.of("main.analytics"))
    print(CatalogSchemaTable.of("main.analytics", ["table"]))
