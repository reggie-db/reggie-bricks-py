"""Helpers for constructing and caching Databricks SDK configuration objects."""

import functools
import json
import os
import pathlib
import subprocess
import threading
from builtins import Exception, ValueError
from configparser import ConfigParser
from enum import Enum
from typing import Any, Callable, Iterable

from databricks.sdk.core import Config
from databricks.sdk.credentials_provider import OAuthCredentialsProvider
from lfp_logging import logs
from pyspark.sql import SparkSession

from dbx_tools import catalogs, clients, runtimes

LOG = logs.logger()

_DEFAULT_DATABRICKS_CONFIG_PROFILE_NAME = "DEFAULT"
_DATABRICKS_AUTH_LOGIN_LOCK = threading.Lock()


def get(profile: str | None = None) -> Config:
    """Return a cached or freshly created Databricks ``Config`` for the given profile.
    When a profile is not provided this function returns a process wide default. A lock is used to avoid concurrent initialization of the default object.
    """
    if not profile:
        profile = _databricks_config_profile_name_default()
    for login in [False, True]:
        if login:
            if _cli_version():
                _DATABRICKS_AUTH_LOGIN_LOCK.acquire()
            else:
                raise ValueError("Databricks CLI not installed")
        try:
            if login:
                _cli_run("auth", "login", profile=profile)
            config = Config(profile=profile)
            break
        except ValueError:
            if login:
                raise
        finally:
            if login:
                _DATABRICKS_AUTH_LOGIN_LOCK.release()
    if not config.cluster_id:
        config.serverless_compute_id = "auto"
    LOG.debug("config created - profile:%s config:%s", profile, config)
    return config


# noinspection PyProtectedMember
def token(config: Config = None) -> str:
    """Extract an API token from the provided or default configuration."""
    if not config:
        config = get()
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
    *args,
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
    proc_args = ["databricks", "--output", "json"]
    proc_args.extend((str(a) for a in args))
    if profile:
        proc_args.extend(["--profile", profile])
    LOG.debug(
        "cli run - args:%s stdout:%s stderr:%s check:%s",
        proc_args,
        stdout,
        stderr,
        check,
    )
    completed_process = subprocess.run(
        proc_args,
        stdout=stdout,
        stderr=stderr,
        check=check,
        timeout=timeout,
        text=True,
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
def _databricks_config_profile_name_default() -> str:
    profile_name = os.environ.get("DATABRICKS_CONFIG_PROFILE", "")
    if not profile_name:
        profile_names = None
        if _cli_version():
            auth_profiles = _cli_run("auth", "profiles")[0]
            if profiles := auth_profiles.get("profiles", []):
                profile_names = [p["name"] for p in profiles]
        else:
            databricks_config_file = pathlib.Path.home() / ".databrickscfg"
            if databricks_config_file.is_file():
                config_parser = ConfigParser()
                config_parser.optionxform = str
                config_files_read = None
                try:
                    config_files_read = config_parser.read(
                        str(databricks_config_file.absolute())
                    )
                except Exception:
                    pass
                if config_files_read:
                    profile_names = config_parser.sections()
                    if config_parser.defaults():
                        profile_names = [
                            _DEFAULT_DATABRICKS_CONFIG_PROFILE_NAME,
                            *profile_names,
                        ]
        if _DEFAULT_DATABRICKS_CONFIG_PROFILE_NAME in profile_names:
            profile_name = _DEFAULT_DATABRICKS_CONFIG_PROFILE_NAME
        elif profile_names and len(profile_names) == 1:
            profile_name = profile_names[0]
    if profile_name:
        LOG.info(f"Databricks config profile name: {profile_name}")
        return profile_name
    else:
        raise ValueError(
            f"Databricks config profile name not found. Set a 'DEFAULT' profile name, limit ~/.databrickscfg to a single profile, or set 'DATABRICKS_CONFIG_PROFILE' environment variable. profile_names:{profile_names}"
        )


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
