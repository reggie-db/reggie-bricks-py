"""Runtime helpers for working within Databricks notebooks and jobs."""

import functools
import json
import os
from copy import deepcopy
from typing import Any, Collection, Mapping, TypeVar

from packaging.version import Version
from pyspark.sql import SparkSession

from dbx_tools import clients

T = TypeVar("T")
_UNSET = object()


@functools.cache
def version() -> Version | None:
    """Return the Databricks runtime version if running on a cluster."""
    if runtime_version := os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return Version(runtime_version)
    return None


def ipython_user_ns(key: str, default_value: T | None = _UNSET) -> T | None:
    if get_ipython_function := _get_ipython_function():
        if ipython := get_ipython_function():
            value = ipython.user_ns.get(key, _UNSET)
            if value is not _UNSET:
                return value
    if default_value is not _UNSET:
        return default_value
    raise KeyError(key)


@functools.cache
def _get_ipython_function():
    """Return the ``get_ipython`` callable when IPython is importable."""
    try:
        from IPython import get_ipython  # pyright: ignore[reportMissingImports]

        return get_ipython
    except ImportError:
        pass


def dbutils() -> "DBUtils":
    """Return the ``DBUtils`` handle associated with the current Spark session."""
    if instance := ipython_user_ns("dbutils", None):
        return instance
    if pyspark_dbutils_class := _pyspark_dbutils_class():
        # noinspection PyTypeChecker
        return pyspark_dbutils_class(clients.spark())
    raise ValueError("DBUtils is not available")


@functools.cache
def _pyspark_dbutils_class():
    """Import and cache the DBUtils entry point when available."""
    try:
        from pyspark.dbutils import DBUtils as PySparkDBUtils

        return PySparkDBUtils
    except ImportError:
        return False


def context(default_value: dict[str, Any] | None = _UNSET) -> dict[str, Any]:
    """Assemble runtime context information from notebook and Spark sources."""
    if get_context_function := _get_context_function():
        if (context_instance := get_context_function()) is not None:
            return deepcopy(context_instance.__dict__)
    dbutils_instance = dbutils()
    if hasattr(dbutils_instance, "entry_point"):
        if (
            context_json := dbutils_instance.entry_point.getDbutils()
            .notebook()
            .getContext()
            .safeToJson()
        ):
            attributes_data = json.loads(context_json).get("attributes", None)
            if attributes_data is not None:

                def _convert(data):
                    if isinstance(data, str):
                        parts = data.split("_")
                        return parts[0] + "".join(p.title() for p in parts[1:])
                    elif isinstance(data, Mapping):
                        return {_convert(k): _convert(v) for k, v in data.items()}
                    elif isinstance(data, Collection):
                        return [_convert(i) for i in data]
                    else:
                        return data

                return _convert(attributes_data)
    if default_value is not _UNSET:
        return default_value
    raise ValueError("Context is not available")


@functools.cache
def _get_context_function():
    """Return the Databricks notebook context accessor when available."""
    try:
        from dbruntime.databricks_repl_context import (
            get_context,  # pyright: ignore[reportMissingImports]
        )

        return get_context
    except ImportError:
        pass


def is_notebook(spark: SparkSession = None) -> bool:
    """Return ``True`` when the current context indicates notebook execution."""
    if not version():
        return False
    return context().get("isInNotebook", False)


def is_job() -> bool:
    """Return ``True`` when the runtime context corresponds to a job run."""
    if not version():
        return False
    return context().get("isInJob", False)


def is_pipeline(spark: SparkSession = None) -> bool:
    """Return ``True`` when executing inside a Databricks pipeline rather than a job."""
    if not version() or is_job():
        return False
    runtime_version = context().get("runtimeVersion", "")
    return runtime_version and runtime_version.startswith("dlt:")
