"""Helpers for constructing and caching Databricks SDK configuration objects."""

import functools
import json
import os
import pathlib
import subprocess
from builtins import Exception, ValueError
from configparser import ConfigParser
from enum import Enum
from typing import Any, Callable, Iterable, TypeVar

from databricks.sdk.core import Config
from databricks.sdk.credentials_provider import OAuthCredentialsProvider
from dbx_core import projects
from dotenv import dotenv_values
from lfp_logging import logs

from dbx_tools import catalogs, clients, runtimes

T = TypeVar("T")
_UNSET = object()
_DEFAULT_DATABRICKS_CONFIG_PROFILE_NAME = "DEFAULT"

LOG = logs.logger()


def get() -> Config:
    """Return a cached or freshly created Databricks ``Config`` for the given profile.
    When a profile is not provided this function returns a process wide default. A lock is used to avoid concurrent initialization of the default object.
    """
    if runtimes.version():
        return Config()
    else:
        return _config()


@functools.cache
def _config() -> Config:
    def _load() -> Config:
        profile = _databricks_config_profile()
        config: Config | None = None
        exc: Exception | None = None
        try:
            config = Config(profile=profile) if profile else Config()
            if not _token(config):
                config = None
        except Exception as e:
            exc = e
        if not config and profile and _cli_version():
            _cli_run("auth", "login", profile=profile, stdout=subprocess.DEVNULL)
            config = Config(profile=profile)
            if not _token(config):
                config = None
        if not config:
            raise exc if exc else ValueError(f"Config not found: {profile}")

        if not config.cluster_id and not config.serverless_compute_id:
            config.serverless_compute_id = "auto"

        return config

    try:
        from wrapt import LazyObjectProxy

        # noinspection PyTypeChecker
        return LazyObjectProxy(_load, interface=Config)
    except ImportError:
        return _load()


def _databricks_config_profile() -> str | None:
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "")
    if not profile:
        profiles = None
        if _cli_version():
            auth_profiles = _cli_run("auth", "profiles")
            if profiles := auth_profiles.get("profiles", []):
                profiles = [p["name"] for p in profiles]
        else:
            databricks_config_file = pathlib.Path.home() / ".databrickscfg"
            if databricks_config_file.is_file():
                config_parser = ConfigParser()
                config_parser.optionxform = str
                config_files_read = None
                # noinspection PyBroadException
                try:
                    config_files_read = config_parser.read(
                        str(databricks_config_file.absolute())
                    )
                except Exception:
                    pass
                if config_files_read:
                    profiles = config_parser.sections()
                    if config_parser.defaults():
                        profiles = [
                            _DEFAULT_DATABRICKS_CONFIG_PROFILE_NAME,
                            *profiles,
                        ]
        if _DEFAULT_DATABRICKS_CONFIG_PROFILE_NAME in profiles:
            profile = _DEFAULT_DATABRICKS_CONFIG_PROFILE_NAME
        elif profiles and len(profiles) == 1:
            profile = profiles[0]
    return profile


# noinspection PyProtectedMember
def token() -> str:
    """Extract an API token from the provided or default configuration."""
    config = get()
    if result := _token(config):
        return result
    raise ValueError(f"config token not found - config:{config}")


def _token(config: Config) -> str | None:
    header_factory = getattr(config, "_header_factory", None)
    if isinstance(header_factory, OAuthCredentialsProvider):
        return config.oauth_token().access_token
    else:
        return config.token


def value(
    name: str,
    default_value: T | None = _UNSET,
    config_value_sources: list["ConfigValueSource"] = None,
) -> T:
    """Fetch a configuration value by checking the configured sources in order.
    The first loader that returns a truthy value wins. Callers can pass a subset of sources to control resolution order.
    """
    if not name:
        raise ValueError("Name required")
    if not config_value_sources:
        config_value_sources = tuple(ConfigValueSource)

    dbutils = (
        runtimes.dbutils()
        if (
            ConfigValueSource.WIDGETS in config_value_sources
            or ConfigValueSource.SECRETS in config_value_sources
        )
        else None
    )

    def _config_value_loaders() -> Iterable[Callable[[str], Any]]:
        for config_value_source in config_value_sources:
            if config_value_source is ConfigValueSource.WIDGETS:
                if widgets := getattr(dbutils, "widgets", None):
                    if (widgets_get := getattr(widgets, "get", None)) and callable(
                        widgets_get
                    ):
                        yield widgets_get
                    # Provide a dict like fallback when widgets is present
                    yield _get_all(widgets).get
            elif config_value_source is ConfigValueSource.SPARK_CONF:
                spark = clients.spark()
                yield spark.conf.get
                # Provide a dict like fallback when Spark conf is present
                yield _get_all(spark, "conf").get
            elif config_value_source is ConfigValueSource.SECRETS:
                if secrets := getattr(dbutils, "secrets", None):
                    if catalog_schema := catalogs.catalog_schema():

                        def _load_secret(key: str) -> str:
                            return secrets.get(scope=str(catalog_schema), key=key)

                        yield _load_secret
            elif config_value_source is ConfigValueSource.OS_ENVIRON:

                def _load(env: dict[str, Any], upper: bool, key: str) -> Any | None:
                    if upper:
                        key_upper = key.upper()
                        if key_upper == key:
                            return None
                        else:
                            key = key_upper
                    return env.get(key, None)

                def _loader(
                    env: dict[str, Any], upper: bool
                ) -> Callable[[str], Any | None]:
                    return lambda key: _load(env, upper, key)

                for upper in (False, True):
                    for env in (os.environ, _env_data()):
                        yield _loader(env, upper)

            else:
                raise ValueError(
                    f"Unknown ConfigValueSource - config_value_source:{config_value_source}"
                )

    for loader in _config_value_loaders():
        # noinspection PyBroadException
        try:
            if value := loader(name):
                return value
        except Exception:
            pass
    if default_value is not _UNSET:
        return default_value
    raise ValueError(f"Config value not found: {name}")


def _cli_run(*args, profile=None, stdout=subprocess.PIPE) -> dict[str, Any]:
    """Execute the Databricks CLI and return the parsed JSON payload and process."""
    if runtimes.version():
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
def _env_data() -> dict[str, Any]:
    env_file_name = ".env"
    env_file_names = [env_file_name]
    if not runtimes.version():
        env_file_names.append(".dev" + env_file_name)
    data = {}
    root_dir = projects.root_dir()
    for env_file_name in env_file_names:
        env_file = root_dir / env_file_name
        if env_file.is_file():
            if env_data := dotenv_values(env_file):
                data.update(env_data)
    return data


class ConfigValueSource(Enum):
    """Enumerates supported config sources in order of discovery precedence."""

    WIDGETS = 2
    SPARK_CONF = 1
    OS_ENVIRON = 3
    SECRETS = 4

    @classmethod
    def without(cls, *excluded):
        """Return members excluding any provided in ``excluded`` while preserving order."""
        return [member for member in cls if member not in excluded]


if __name__ == "__main__":
    print(_cli_version())
    print(value("COOL"))
