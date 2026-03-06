"""Runtime helpers for working within Databricks notebooks and jobs."""

import json
import os
import re
from copy import deepcopy
from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, Any, Collection, Mapping

from dbx_core import imports, strs
from lfp_logging import logs
from lfp_types import T
from packaging.version import InvalidVersion, Version

from dbx_tools import clients

LOG = logs.logger()
_UNSET = object()

if TYPE_CHECKING:
    from pyspark.dbutils import DBUtils


@dataclass
class AppInfo:
    name: str
    url: str
    port: int


def version() -> Version | None:
    """Return the Databricks runtime version if running on a cluster."""
    if runtime_version_env := os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        runtime_version = _runtime_version(runtime_version_env)
        LOG.debug(f"Runtime Version: {runtime_version}")
        return runtime_version
    return None


def _runtime_version(version_value: str) -> Version:
    """Parse Databricks runtime version with fallback normalization.

    The Databricks runtime environment can sometimes expose values that are not
    valid PEP 440 versions (for example ``client.5.0``). In that case we
    normalize to a compatible form like ``0.5.0+client``.
    """
    try:
        return Version(version_value)
    except InvalidVersion:
        pass

    version_text = str(version_value).strip()
    if not version_text:
        raise InvalidVersion("Invalid version: ''")

    # Capture a leading non-numeric label and trailing numeric portion.
    match = re.match(r"^(?P<label>[^\d]+)[\._-]*(?P<num>\d[\d\._-]*)$", version_text)
    if match:
        label = ".".join(strs.tokenize(match.group("label")))
        numeric = match.group("num").replace("_", ".").replace("-", ".")
        normalized = f"0.{numeric}"
        if label:
            normalized = f"{normalized}+{label}"
        return Version(normalized)

    # Fallback to a strict parse to preserve existing failure semantics.
    return Version(version_text)


def app_info() -> AppInfo | None:
    """Return the application information associated with the current cluster."""
    data = {}
    for f in fields(AppInfo):
        field_name = f.name
        value = os.environ.get(f"DATABRICKS_APP_{field_name.upper()}", None)
        if value is None:
            return None
        try:
            data[field_name] = f.type(value)
        except (TypeError, ValueError):
            return None
    return AppInfo(**data)


def ipython_user_ns(key: str, default_value: T | None = _UNSET) -> T | None:
    """Return a value from the active IPython user namespace when available.

    Args:
        key: Namespace key to resolve.
        default_value: Value returned when key or IPython context is unavailable.

    Returns:
        Namespace value for ``key`` or ``default_value`` when provided.

    Raises:
        KeyError: When no value is found and no default is supplied.
    """
    if get_ipython_function := imports.resolve("IPython", "get_ipython"):
        if ipython := get_ipython_function():
            value = ipython.user_ns.get(key, _UNSET)
            if value is not _UNSET:
                return value
    if default_value is not _UNSET:
        return default_value
    raise KeyError(key)


def ipython(default_value: Any | None = _UNSET) -> Any | None:
    """Return the active IPython shell object.

    This preserves the legacy helper that tests and downstream callers import.

    Args:
        default_value: Value returned when no active IPython shell exists.

    Returns:
        Active IPython shell object or ``default_value``.

    Raises:
        ValueError: When IPython is unavailable and no default is supplied.
    """
    if get_ipython_function := imports.resolve("IPython", "get_ipython"):
        if shell := get_ipython_function():
            return shell
    if default_value is not _UNSET:
        return default_value
    raise ValueError("IPython is not available")


# noinspection PyUnresolvedReferences,PyTypeHints
def dbutils(spark: bool = True) -> "DBUtils | None":
    """Return the ``DBUtils`` handle associated with the current Spark session.

    Args:
        spark: When ``True``, attempt construction from a Spark session when a
            notebook-injected ``dbutils`` handle is not available.

    Returns:
        ``DBUtils`` instance when available, otherwise ``None``.
    """
    if instance := ipython_user_ns("dbutils", None):
        return instance
    if spark:
        if pyspark_dbutils_class := imports.resolve("pyspark.dbutils", "DBUtils"):
            # noinspection PyTypeChecker
            return pyspark_dbutils_class(clients.spark())
    return None


def context(default_value: dict[str, Any] | None = _UNSET) -> dict[str, Any]:
    """Assemble runtime context information from notebook and Spark sources.

    Args:
        default_value: Value returned when context cannot be resolved.

    Returns:
        Runtime context dictionary.

    Raises:
        ValueError: When context is unavailable and no default is supplied.
    """
    if get_context_function := imports.resolve(
        "dbruntime.databricks_repl_context", "get_context"
    ):
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


def is_notebook() -> bool:
    """Return ``True`` when the current context indicates notebook execution."""
    if not version():
        return False
    return context().get("isInNotebook", False)


def is_job() -> bool:
    """Return ``True`` when the runtime context corresponds to a job run."""
    if not version():
        return False
    return context().get("isInJob", False)


def is_pipeline() -> bool:
    """Return ``True`` when executing inside a Databricks pipeline rather than a job."""
    if not version() or is_job():
        return False
    runtime_version = context().get("runtimeVersion", "")
    return runtime_version and runtime_version.startswith("dlt:")
