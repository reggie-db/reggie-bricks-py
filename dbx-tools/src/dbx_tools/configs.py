"""Helpers for constructing and caching Databricks SDK configuration objects."""

import base64
import functools
import json
import os
import subprocess
from enum import Enum
from typing import Any, Callable, Iterable, Mapping, TypeVar

from databricks.sdk.core import Config
from databricks.sdk.credentials_provider import OAuthCredentialsProvider
from dbx_core import imports, projects, strs
from lfp_logging import logs

from dbx_tools import catalogs, clients, runtimes

T = TypeVar("T")
_UNSET = object()

LOG = logs.logger()


def get() -> Config:
    """Return the Databricks SDK ``Config`` used by workspace clients."""
    if runtimes.version() or runtimes.app_info():
        config = Config()
    else:
        config = _get()
    if not config.cluster_id and not config.serverless_compute_id:
        config.serverless_compute_id = "auto"
    return config


@functools.cache
def _get() -> Config:
    def _load() -> Config:
        # Delegate to core config
        return clients.workspace_client().config

    if lazy_object_proxy := imports.resolve("wrapt", "LazyObjectProxy"):
        return lazy_object_proxy(_load, interface=Config)
    else:
        return _load()


def token(config: Config | None = None) -> str | None:
    """Return an access token for a Databricks SDK `Config`.

    Notes:
    - `get()` is responsible for loading the default configuration with runtime
      awareness and workspace client derived authentication.
    - This function only extracts a token from a `Config` instance and does not log
      sensitive values.
    """

    if config is None:
        config = get()

    header_factory = getattr(config, "_header_factory", None)
    if isinstance(header_factory, OAuthCredentialsProvider):
        return config.oauth_token().access_token

    if token := config.token:
        return token

    # Some auth modes can still expose `oauth_token()` without the header factory type.
    oauth_token = getattr(config, "oauth_token", None)
    if callable(oauth_token):
        return oauth_token().access_token

    raise ValueError(f"Config token not found - config:{config}")


def value(
    name: str,
    default_value: T | None = _UNSET,
    bundle_path: str | None = None,
    config_value_sources: list["ConfigValueSource"] = None,
) -> T:
    """Fetch a configuration value by checking the configured sources in order.
    The first loader that returns a truthy value wins. Callers can pass a subset of sources to control resolution order.
    """
    if not name:
        raise ValueError("Name required")
    if not config_value_sources:
        config_value_sources = tuple(ConfigValueSource)

    for loader in _value_loaders(config_value_sources):
        # noinspection PyBroadException
        try:
            if value := loader(name):
                return value
        except Exception:
            pass
    if _cli_version():
        if data := _bundle_data():
            if not bundle_path:
                bundle_path = f"variables.{name}"
            parts = bundle_path.split(".")
            for idx, part in enumerate(parts):
                if isinstance(data, Mapping):
                    value = data.get(part, None)
                    last = idx == len(parts) - 1
                    if not last:
                        data = value
                    else:
                        is_variable = "variables" == parts[0]
                        if is_variable:
                            if isinstance(value, Mapping):
                                value = value.get("value", None)
                            else:
                                value = None
                        if isinstance(value, str) and value:
                            return value

    if default_value is not _UNSET:
        return default_value
    raise ValueError(f"Config value not found: {name}")


def _value_loaders(
    config_value_sources: Iterable["ConfigValueSource"],
) -> Iterable[Callable[[str], Any]]:
    """Yield callables that resolve keys from each configured source.

    Args:
        config_value_sources: Ordered collection of sources to inspect.

    Yields:
        Callables that accept a key and return a resolved value when available.
    """
    for config_value_source in config_value_sources:
        if config_value_source is ConfigValueSource.WIDGETS:
            if dbutils := runtimes.dbutils(spark=False):
                widgets = getattr(dbutils, "widgets", None) if dbutils else None
                if widgets:
                    if (widgets_get := getattr(widgets, "get", None)) and callable(
                        widgets_get
                    ):
                        yield widgets_get
                    # Provide a dict like fallback when widgets is present
                    yield _get_all(widgets).get
        elif config_value_source is ConfigValueSource.SPARK_CONF:
            if spark := clients.spark(connect=False):
                yield spark.conf.get
                # Provide a dict like fallback when Spark conf is present.
                yield _get_all(spark, "conf").get
        elif config_value_source is ConfigValueSource.SECRETS:
            if catalog_schema := catalogs.catalog_schema():
                if dbutils := runtimes.dbutils(spark=False):
                    if secrets := getattr(dbutils, "secrets", None):

                        def _load_secret(key: str) -> str:
                            return secrets.get(scope=str(catalog_schema), key=key)

                        yield _load_secret
                else:
                    wc = clients.workspace_client()

                    def _load_secret(key: str) -> str | None:
                        secret = wc.secrets.get_secret(
                            scope=str(catalog_schema), key=key
                        )
                        if secret and secret.value:
                            return base64.b64decode(secret.value).decode("utf-8")
                        return None

                    yield _load_secret

        elif config_value_source is ConfigValueSource.OS_ENVIRON:
            yield os.getenv
            yield lambda k: os.getenv(k.upper())
            yield lambda k: os.getenv("_".join(strs.tokenize(k)).upper())

        else:
            raise ValueError(
                f"Unknown ConfigValueSource - config_value_source:{config_value_source}"
            )


def _cli_run(*args, profile=None, stdout=subprocess.PIPE) -> dict[str, Any]:
    """Execute the Databricks CLI and return parsed JSON output.

    Args:
        *args: CLI subcommands and arguments to pass after ``databricks --output json``.
        profile: Optional CLI profile name.
        stdout: Stream destination used by ``subprocess.run``.

    Returns:
        Parsed JSON response payload.
    """
    if runtimes.version() or runtimes.app_info():
        return {}
    proc_args = ["databricks", "--output", "json"]
    proc_args.extend((str(a) for a in args))
    if profile:
        proc_args.extend(["--profile", profile])
    proc = subprocess.run(
        proc_args,
        stdout=stdout,
        check=True,
        text=True,
    )
    return json.loads(proc.stdout)


def _get_all(obj: Any, *attribues: str) -> dict[str, Any]:
    """Safely traverse attributes and callables to a final object with getAll then normalize the result to a dict.
    This supports Spark and DBUtils objects that expose nested accessors and return either a dict or an iterable of pairs.

    Args:
        obj: Root object to inspect.
        *attribues: Attribute path to traverse before reading ``getAll``.

    Returns:
        Dictionary of extracted key/value pairs.
    """
    data = {}
    for i in range(len(attribues) + 1):
        if i > 0:
            obj = getattr(obj, attribues[i - 1], None)
        if callable(obj):
            obj = obj()
        if obj is None:
            return data
    getAll = getattr(obj, "getAll", None)
    if isinstance(getAll, dict):
        return getAll
    elif callable(getAll):
        try:
            conf_all = getAll()
        except Exception:
            return data
    else:
        return data
    if isinstance(conf_all, Iterable):
        for value in conf_all:
            if isinstance(value, tuple) and len(value) == 2:
                data[value[0]] = value[1]
    return data


@functools.cache
def _cli_version() -> dict[str, Any]:
    """Return cached CLI version metadata, if available outside a runtime cluster."""
    version = _cli_run("version")
    LOG.debug(f"CLI Version: {version}")
    return version


@functools.cache
def _bundle_data() -> dict[str, Any]:
    root_dir = projects.root_dir()
    bundle_file = root_dir / "databricks.yml"
    if bundle_file.is_file():
        # noinspection PyBroadException
        try:
            proc = subprocess.run(
                ["databricks", "bundle", "--output", "json", "validate"],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
            if proc.returncode == 0:
                return json.loads(proc.stdout)
        except Exception:
            pass
    return {}


class ConfigValueSource(Enum):
    """Enumerates supported config sources in order of discovery precedence."""

    WIDGETS = 1
    SPARK_CONF = 2
    SECRETS = 3
    OS_ENVIRON = 4

    @classmethod
    def without(cls, *excluded):
        """Return members excluding any provided in ``excluded`` while preserving order."""
        return [member for member in cls if member not in excluded]
