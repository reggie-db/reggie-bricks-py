"""Runtime helpers for working within Databricks notebooks and jobs."""

import builtins
import functools
import json
import os
import threading
from typing import Any, Callable, TypeVar

from pyspark.sql import SparkSession

from dbx_tools import clients

T = TypeVar("T")
_INSTANCE_CACHE: dict[str, Any] = {}
_INSTANCE_LOCK = threading.RLock()


@functools.cache
def version() -> str | None:
    """Return the Databricks runtime version if running on a cluster."""
    runtime_version = os.environ.get("DATABRICKS_RUNTIME_VERSION")
    return runtime_version or None


def instance(name: str, factory: Callable[[], T] | None) -> T | None:
    def _instance(locked: bool = False) -> T:
        result: T | None = _INSTANCE_CACHE.get(name, None)
        if result is None and not locked:
            user_ns = _ipython_user_ns()
            if user_ns is not None:
                result = user_ns.get(name, None)
            if result is None:
                result = getattr(builtins, name, None)
        if result is None and factory is not None:
            if not locked:
                _INSTANCE_LOCK.acquire()
                try:
                    return _instance(True)
                finally:
                    _INSTANCE_LOCK.release()
            result = factory()
            if result is not None:
                _INSTANCE_CACHE[name] = result
        return result

    return _instance()


def _ipython_user_ns() -> dict[str, Any] | None:
    if get_ipython_fn := _get_ipython_fn():
        if ipython := get_ipython_fn():
            return ipython.user_ns
    return None


@functools.cache
def _get_ipython_fn():
    """Return the ``get_ipython`` callable when IPython is importable."""
    try:
        from IPython import get_ipython  # pyright: ignore[reportMissingImports]

        return get_ipython
    except ImportError:
        pass


def dbutils(spark: SparkSession = None):
    """Return the ``DBUtils`` handle associated with the current Spark session."""

    def _load(spark_instance: SparkSession):
        if dbutils_class := _dbutils_class():
            return dbutils_class(spark_instance)
        return None

    if spark is None:
        return instance("dbutils", lambda: _load(clients.spark()))
    return _load(spark)


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
