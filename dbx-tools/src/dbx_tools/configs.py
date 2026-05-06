"""Helpers for constructing and caching Databricks SDK configuration objects."""

import base64
import functools
import json
import os
import subprocess
from enum import Enum
from typing import Any, Callable, Iterable, Mapping, overload

import lfp_types
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from dbx_core import imports, projects, strs
from lfp_logging import logs

from dbx_tools import catalogs, clients, runtimes

LOG = logs.logger()

_UNSET: Any = object()
_DEFAULT_PROFILE_NAME = "DEFAULT"


class ConfigValueSource(Enum):
    """Enumerates supported config sources in order of discovery precedence."""

    WIDGETS = 1
    SPARK_CONF = 2
    SECRETS = 3
    OS_ENVIRON = 4
    BUNDLE = 5
    CLI = 6

    @classmethod
    def default(
        cls, exclude: Iterable["ConfigValueSource"] | None = None
    ) -> list["ConfigValueSource"]:
        """Return the default config value sources."""
        if exclude:
            exclude = lfp_types.to_container(exclude)
        return [
            member
            for member in cls
            if ((not exclude) or (member not in exclude) and member != cls.CLI)
        ]


def get(profile_name: str | None = None) -> Config:
    """Return the Databricks SDK ``Config`` used by workspace clients."""
    if profile_name:
        config = Config(profile_name=profile_name)
    elif runtimes.version() or runtimes.app_info():
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


def profile() -> str | None:
    """Return the active CLI profile name."""
    if cfg_profile := os.environ.get("DATABRICKS_CONFIG_PROFILE", None):
        return cfg_profile
    names = profiles()
    if len(names) == 1:
        name = next(iter(names))
    elif _DEFAULT_PROFILE_NAME in names:
        name = _DEFAULT_PROFILE_NAME
    else:
        return None
    os.environ["DATABRICKS_CONFIG_PROFILE"] = name
    return name


@functools.cache
def profiles() -> list[str]:
    """Return available CLI profile names."""
    names: list[str] = []
    if auth_profiles := _cli_run(
        "auth", "profiles", "--skip-validate", "--output", "json"
    ):
        profiles_data = auth_profiles.get("profiles", None)
        if profiles_data and isinstance(profiles_data, list):
            for profile_data in profiles_data:
                name = profile_data.get("name", None)
                if name and name not in names:
                    if _DEFAULT_PROFILE_NAME == name:
                        names.insert(0, name)
                    else:
                        names.append(name)
    return names


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

    if getattr(config, "_header_factory", None):
        oauth_token_fn = getattr(config, "oauth_token", None)
        if callable(oauth_token_fn):
            if oauth_token := oauth_token_fn():
                if access_token := getattr(oauth_token, "access_token", None):
                    return access_token

    if token := config.token:
        return token

    # Some auth modes can still expose `oauth_token()` without the header factory type.
    oauth_token = getattr(config, "oauth_token", None)
    if callable(oauth_token):
        if oauth_token_value := oauth_token():
            if access_token := getattr(oauth_token_value, "access_token", None):
                return access_token

    raise ValueError(f"Config token not found - config:{config}")


@overload
def value(
    name: str,
    *,
    bundle_path: str | None = None,
    workspace_client: WorkspaceClient | None = None,
    config_value_sources: Iterable[ConfigValueSource] | None = None,
) -> str: ...


@overload
def value(
    name: str,
    default_value: str,
    bundle_path: str | None = None,
    workspace_client: WorkspaceClient | None = None,
    config_value_sources: Iterable[ConfigValueSource] | None = None,
) -> str: ...


@overload
def value(
    name: str,
    default_value: None,
    bundle_path: str | None = None,
    workspace_client: WorkspaceClient | None = None,
    config_value_sources: Iterable[ConfigValueSource] | None = None,
) -> str | None: ...


def value(
    name: str,
    default_value: str | None = _UNSET,
    bundle_path: str | None = None,
    workspace_client: WorkspaceClient | None = None,
    config_value_sources: Iterable[ConfigValueSource] | None = None,
) -> str | None:
    """Fetch a configuration value by checking the configured sources in order.

    Every supported source (widgets, Spark conf, secrets, environment, bundle
    variables) returns strings, so the resolved value is always a ``str``. The
    first loader that yields a truthy value wins. Callers can restrict the
    resolution order by passing a subset of sources.

    Args:
        name: Config key to look up.
        default_value: Fallback string returned when no source resolves a value.
            When ``None`` (the parameter default), a missing value raises
            ``ValueError`` instead of returning silently.
        bundle_path: Override path used to look up the value in bundle data
            (defaults to ``variables.{name}``).
        config_value_sources: Ordered subset of sources to consult.

    Returns:
        The resolved configuration string, or ``default_value`` when no source
        produced one and a non-``None`` default was supplied.

    Raises:
        ValueError: When ``name`` is empty, or when no source resolves a value
            and ``default_value`` is ``None``.
    """
    if not name:
        raise ValueError("Name required")
    if not config_value_sources:
        config_value_sources = ConfigValueSource.default()
    elif not isinstance(config_value_sources, set):
        config_value_sources = list(dict.fromkeys(config_value_sources))

    for config_value_source in config_value_sources:
        try:
            for loader in _value_loaders(
                config_value_source,
                name,
                default_value=default_value,
                bundle_path=bundle_path,
                workspace_client=workspace_client,
            ):
                try:
                    value = loader()
                    if isinstance(value, str) and value:
                        return value
                except Exception:
                    pass
        except Exception:
            LOG.warning(
                f"Value loader failed for {config_value_source}: {name}", exc_info=True
            )
            pass
    if default_value is _UNSET:
        raise ValueError(f"Config value not found: {name}")
    return default_value


def _value_loaders(
    config_value_source: ConfigValueSource,
    name: str,
    default_value: str | None,
    bundle_path: str | None,
    workspace_client: WorkspaceClient | None = None,
) -> Iterable[Callable[[], Any]]:
    if config_value_source is ConfigValueSource.WIDGETS:
        if dbutils := runtimes.dbutils(spark=False):
            if widgets := getattr(dbutils, "widgets", None):
                yield lambda: _get_all(widgets).get(name)
    elif config_value_source is ConfigValueSource.SPARK_CONF:
        if spark := clients.spark(connect=False):
            yield lambda: spark.conf.get(name)
            # Provide a dict like fallback when Spark conf is present.
            yield lambda: _get_all(spark, "conf").get(name)
    elif config_value_source is ConfigValueSource.SECRETS:
        if catalog_schema := catalogs.catalog_schema():
            if dbutils := runtimes.dbutils(spark=False):
                if hasattr(dbutils, "secrets"):
                    return lambda: dbutils.secrets.get(
                        scope=str(catalog_schema), key=name
                    )

            def _load_workspace_client_secret_value() -> Any:
                secret = (
                    workspace_client or clients.workspace_client()
                ).secrets.get_secret(scope=str(catalog_schema), key=name)
                if secret and secret.value:
                    return base64.b64decode(secret.value).decode("utf-8")
                return None

            yield _load_workspace_client_secret_value
    elif config_value_source is ConfigValueSource.OS_ENVIRON:
        yield lambda: os.getenv(name)
        yield lambda: os.getenv(name.upper())
        yield lambda: os.getenv("_".join(strs.tokenize(name)).upper())
    elif config_value_source is ConfigValueSource.BUNDLE:

        def _load_bundle_value() -> Any:
            if _cli_version() and (data := _bundle_data()):
                parts = (bundle_path or f"variables.{name}").split(".")
                current: Any = data
                for idx, part in enumerate(parts):
                    if isinstance(current, Mapping):
                        bundle_value = current.get(part, None)
                        last = idx == len(parts) - 1
                        if not last:
                            current = bundle_value
                        else:
                            is_variable = "variables" == parts[0]
                            if is_variable:
                                if isinstance(bundle_value, Mapping):
                                    return bundle_value.get("value", None)
                                else:
                                    return None
                            else:
                                return bundle_value

        yield _load_bundle_value
    elif config_value_source is ConfigValueSource.CLI:

        def _load_cli_value() -> Any:
            if default_value is None:
                return input(f"Enter value for {name}: ")

        yield _load_cli_value


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
            proc_args = ["databricks"]
            if profile_name := profile():
                proc_args.extend(["--profile", profile_name])
            proc_args.extend(["bundle", "--output", "json", "validate"])
            proc = subprocess.run(
                proc_args,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
            if proc.returncode == 0:
                return json.loads(proc.stdout)
        except Exception:
            pass
    return {}


# if __name__ == "__main__":
#     print(value("username", config_value_sources=ConfigValueSource.default()+[ConfigValueSource.CLI]))
