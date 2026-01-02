"""Runtime helpers for working within Databricks notebooks and jobs."""

import functools
import json
import os
from typing import Any

from pyspark.sql import SparkSession

from reggie_tools import clients


@functools.cache
def version() -> str | None:
    """Return the Databricks runtime version if running on a cluster."""
    runtime_version = os.environ.get("DATABRICKS_RUNTIME_VERSION")
    return runtime_version or None


def ipython() -> Any | None:
    """Return the active IPython instance when executing inside a notebook."""
    if get_ipython_function := _get_ipython_function():
        if ip := get_ipython_function():
            return ip
    return None


def ipython_user_ns(name: str) -> Any | None:
    """Look up ``name`` within the IPython user namespace, if available."""
    if ip := ipython():
        return ip.user_ns.get(name)
    return None


def dbutils(spark: SparkSession = None):
    """Return the ``DBUtils`` handle associated with the current Spark session."""
    if not spark:
        if dbutils := ipython_user_ns("dbutils"):
            return dbutils
        spark = clients.spark()
    if dbutils_class := _dbutils_class():
        # Construct DBUtils using the detected class for the current session
        if dbutils := dbutils_class(spark):
            return dbutils
    return None


def context(spark: SparkSession = None) -> dict[str, Any]:
    """Assemble runtime context information from notebook and Spark sources."""
    contexts: list[dict[str, Any]] = []
    if get_context_function := _get_context_function():
        contexts.append(get_context_function().__dict__)
    context_dbutils = dbutils(spark)
    if context_dbutils and hasattr(context_dbutils, "entry_point"):
        context_json = (
            context_dbutils.entry_point.getDbutils()
            .notebook()
            .getContext()
            .safeToJson()
        )
        contexts.append(json.loads(context_json).get("attributes", {}))

    def _snake_to_camel(s: str) -> str:
        parts = s.split("_")
        return parts[0] + "".join(p.title() for p in parts[1:])

    def _convert_keys(obj: Any) -> Any:
        if isinstance(obj, dict):
            return {_snake_to_camel(k): _convert_keys(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [_convert_keys(i) for i in obj]
        else:
            return obj

    def _deep_merge(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
        # Apply updates from b into a while recursing into sub dicts
        for k, v in b.items():
            if not v:  # skip falsy
                continue
            if k in a and isinstance(a[k], dict) and isinstance(v, dict):
                a[k] = _deep_merge(a[k], v)
            else:
                a[k] = v

        return a

    context: dict[str, Any] = {}
    for ctx in contexts:
        if ctx:
            context = _deep_merge(_convert_keys(context), ctx)
    return context


def is_notebook(spark: SparkSession = None) -> bool:
    """Return ``True`` when the current context indicates notebook execution."""
    ctx = context(spark)
    return ctx.get("isInNotebook", False)


def is_job(spark: SparkSession = None) -> bool:
    """Return ``True`` when the runtime context corresponds to a job run."""
    ctx = context(spark)
    return ctx.get("isInJob", False)


def is_pipeline(spark: SparkSession = None) -> bool:
    """Return ``True`` when executing inside a Databricks pipeline rather than a job."""
    if is_job(spark):
        return False
    ctx = context(spark)
    runtime_version = ctx.get("runtimeVersion", "")
    return runtime_version and runtime_version.startswith("dlt:")


@functools.cache
def _dbutils_class():
    """Import and cache the DBUtils entry point when available."""
    try:
        from pyspark.dbutils import DBUtils

        return DBUtils
    except ImportError:
        return False


@functools.cache
def _get_ipython_function():
    """Return the ``get_ipython`` callable when IPython is importable."""
    try:
        from IPython import get_ipython  # pyright: ignore[reportMissingImports]

        return get_ipython
    except ImportError:
        pass


@functools.cache
def _get_context_function():
    """Return the Databricks notebook context accessor when available."""
    try:
        from dbruntime.databricks_repl_context import (  # pyright: ignore[reportMissingImports]
            get_context,
        )

        return get_context
    except ImportError:
        pass


if __name__ == "__main__":
    pass
