"""Helpers for constructing and caching Databricks SDK configuration objects."""

import functools
import json
import os
import subprocess
import threading
from builtins import Exception, ValueError
from enum import Enum
from typing import Any, Callable, Iterable

from databricks.sdk.core import Config
from databricks.sdk.credentials_provider import OAuthCredentialsProvider
from pyspark.sql import SparkSession
from reggie_core import inputs, logs

from reggie_tools import catalogs, clients, runtimes

LOG = logs.logger(__file__)

_config_default_lock = threading.Lock()
_config_default: Config | None = None


def get(profile: str | None = None) -> Config:
    """Return a cached or freshly created Databricks ``Config`` for the given profile.
    When a profile is not provided this function returns a process wide default. A lock is used to avoid concurrent initialization of the default object.
    """
    global _config_default
    if not profile:
        profile = None
    if not profile:
        if _config_default:
            return _config_default
        elif not _config_default_lock.locked():
            with _config_default_lock:
                return get(profile)

    def _default_profile() -> str | None:
        if not _cli_version():
            return None
        auth_profiles = _cli_auth_profiles()
        profiles = auth_profiles.get("profiles", [])
        if profiles:
            for profile in profiles:
                profile_name = profile.get("name")
                if "DEFAULT" == profile_name:
                    return profile_name
            profile_names = [p["name"] for p in profiles]
            if len(profile_names) > 1:
                return inputs.select_choice("Select a profile", profile_names)
            elif profile_names:
                return profile_names[0]
        return None

    def _config(profile: str | None, auth_login: bool = True) -> Config:
        try:
            cfg = Config(profile=profile)
            if not cfg.cluster_id:
                cfg.serverless_compute_id = "auto"
            return cfg
        except Exception as e:
            # If profile is empty, try to discover a default profile
            if not profile:
                profile = _default_profile()
                if profile:
                    return _config(profile, auth_login)
            # Attempt a one time login then retry config creation
            if auth_login and profile:
                _cli_auth_login(profile)
                return _config(profile, False)
            raise e

    cfg = _config(profile)
    LOG.debug("config created - profile:%s config:%s", profile, cfg)
    if not profile:
        _config_default = cfg
    return cfg


def token(config: Config = None) -> str:
    """Extract an API token from the provided or default configuration."""
    if not config:
        config = globals().get("config")()
    if isinstance(config._header_factory, OAuthCredentialsProvider):
        return config.oauth_token().access_token
    else:
        if config.token:
            return config.token
        else:
            raise ValueError(f"config token not found - config:{config}")


def config_value(
    name: str,
    default: Any = None,
    spark: SparkSession = None,
    config_value_sources: list["ConfigValueSource"] = None,
) -> Any:
    """Fetch a configuration value by checking the configured sources in order.
    The first loader that returns a truthy value wins. Callers can pass a subset of sources to control resolution order.
    """
    if not name:
        raise ValueError("name cannot be empty")
    if not config_value_sources:
        config_value_sources = tuple(ConfigValueSource)

    dbutils = (
        runtimes.dbutils(spark)
        if (
            ConfigValueSource.WIDGETS in config_value_sources
            or ConfigValueSource.SECRETS in config_value_sources
        )
        else None
    )

    def _config_value_loaders() -> Iterable[Callable[[str], Any]]:
        for config_value_source in config_value_sources:
            if config_value_source is ConfigValueSource.WIDGETS:
                widgets = getattr(dbutils, "widgets", None)
                widgets_get = getattr(widgets, "get", None)
                if callable(widgets_get):
                    yield widgets.get
                # Provide a dict like fallback when widgets is present
                yield _get_all(widgets).get
            elif config_value_source is ConfigValueSource.SPARK_CONF:
                config_spark = spark or clients.spark()
                yield config_spark.conf.get
                # Provide a dict like fallback when Spark conf is present
                yield _get_all(config_spark, "conf").get
            elif config_value_source is ConfigValueSource.OS_ENVIRON:
                yield os.environ.get
            elif config_value_source is ConfigValueSource.SECRETS:
                secrets = getattr(dbutils, "secrets", None)
                if secrets:
                    if catalog_schema := catalogs.catalog_schema(spark):

                        def _load_secret(key: str) -> str:
                            return secrets.get(scope=str(catalog_schema), key=key)

                        yield _load_secret
            else:
                raise ValueError(
                    f"unknown ConfigValueSource - config_value_source:{config_value_source}"
                )

    for loader in _config_value_loaders():
        try:
            if value := loader(name):
                return value
        except Exception:
            pass
    return default


def _cli_run(
    *popenargs,
    profile=None,
    stdout=subprocess.PIPE,
    stderr=None,
    check=False,
    timeout=None,
) -> tuple[dict[str, Any], subprocess.CompletedProcess]:
    """Execute the Databricks CLI and return the parsed JSON payload and process."""
    version = runtimes.version()
    if version:
        raise ValueError("cli unsupported in databricks runtime - version:{version}")
    args = ["databricks", "--output", "json"]
    args.extend(popenargs)
    if profile:
        args.extend(["--profile", profile])
    LOG.debug(
        "cli run - args:%s stdout:%s stderr:%s check:%s", args, stdout, stderr, check
    )
    completed_process = subprocess.run(
        args, stdout=stdout, stderr=stderr, check=check, timeout=timeout
    )
    return json.loads(
        completed_process.stdout
    ) if completed_process.stdout else None, completed_process


def _get_all(obj: Any, *attribues: str) -> dict[str, Any]:
    """Safely traverse attributes and callables to a final object with getAll then normalize the result to a dict.
    This supports Spark and DBUtils objects that expose nested accessors and return either a dict or an iterable of pairs.
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
    version = (
        None
        if runtimes.version()
        else _cli_run("version", check=False, stderr=subprocess.DEVNULL)[0]
    )
    LOG.debug(f"version:{version}")
    return version


@functools.cache
def _cli_auth_profiles() -> dict[str, Any | None]:
    """Return cached authentication profiles discovered via the Databricks CLI."""
    auth_profiles = _cli_run("auth", "profiles")[0]
    LOG.debug(f"auth profiles:{auth_profiles}")
    return auth_profiles


def _cli_auth_login(profile: str):
    """Execute ``databricks auth login`` for the provided CLI profile."""
    _cli_run("auth", "login", profile=profile)


class ConfigValueSource(Enum):
    """Enumerates supported config sources in order of discovery precedence."""

    WIDGETS = 1
    SPARK_CONF = 2
    OS_ENVIRON = 3
    SECRETS = 4

    @classmethod
    def without(cls, *excluded):
        """Return members excluding any provided in ``excluded`` while preserving order."""
        return [member for member in cls if member not in excluded]
