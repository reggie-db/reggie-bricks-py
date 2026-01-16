import enum
import functools
import hashlib
import os
import re
import shlex
import socket
from typing import Iterable

import dynaconf
from dbx_core import objects, parsers, strs
from lfp_logging import logs

from dbx_app_runner import conda, docker, git

LOG = logs.logger()


DATABRICKS_APP_ENV_VAR_PREFIX = "DATABRICKS_"
DATABRICKS_APP_PORT_ENV_VAR = DATABRICKS_APP_ENV_VAR_PREFIX + "APP_PORT"
DATABRICKS_APP_ENV_VARS = [
    DATABRICKS_APP_ENV_VAR_PREFIX + name
    for name in [
        "APP_NAME",
        "WORKSPACE_ID",
        "HOST",
        "CLIENT_ID",
        "CLIENT_SECRET",
    ]
]

_APP_RUNNER_PREFIX = "DATABRICKS_APP_RUNNER"
_APP_RUNNER_DEFAULT_NAME = "DEFAULT"


def read_configs(**kwargs) -> Iterable["AppRunnerConfig"]:
    kwargs.setdefault("dotenv", True)
    settings_file = []
    for ext in ["toml", "json", "yaml"]:
        settings_file.append(f"settings.{ext}")
    kwargs.setdefault("settings_file", settings_file)
    app_name_to_prefix: dict[str, str] = {}
    root_prefix_pattern = re.compile(rf"^{_APP_RUNNER_PREFIX}_[^_]")
    app_prefix_pattern = re.compile(rf"^({_APP_RUNNER_PREFIX}__(\S+)_)_[^_]")
    databricks_env = {k: os.environ.get(k) for k in DATABRICKS_APP_ENV_VARS}
    for name in os.environ:
        if match := re.match(root_prefix_pattern, name):
            app_name_to_prefix[""] = _APP_RUNNER_PREFIX
        elif match := re.match(app_prefix_pattern, name):
            app_name = match.group(2)
            if not _APP_RUNNER_DEFAULT_NAME == app_name:
                app_name_to_prefix[app_name] = match.group(1)
    app_runner_default_prefix = f"{_APP_RUNNER_PREFIX}__{_APP_RUNNER_DEFAULT_NAME}_"
    for app_name, app_prefix in app_name_to_prefix.items():
        envar_prefixes = [
            app_runner_default_prefix,
            app_prefix,
        ]
        app_databricks_env = databricks_env.copy()
        app_databricks_env[DATABRICKS_APP_PORT_ENV_VAR] = _random_port()

        settings = dynaconf.Dynaconf(
            envvar_prefix=",".join(envar_prefixes),
            merge_enabled=True,
            **app_databricks_env,
            **kwargs,
        )
        app_runner_config = AppRunnerConfig(settings, name=app_name)
        yield app_runner_config


def _random_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


class AppRunnerSource(enum.IntEnum):
    GITHUB = 1
    DOCKER = 2
    LOCAL = 3


class AppRunnerConfig:
    ROOT_NAME = "ROOT"

    def __init__(self, settings: dynaconf.Dynaconf, name: str = None):
        self.settings = settings
        self._name = name

    @property
    def name(self) -> str:
        return self._name or self.ROOT_NAME

    @property
    def type(self) -> AppRunnerSource:
        source = self.source.strip() if self.source else None
        if source:
            if git.is_url(self.source):
                return AppRunnerSource.GITHUB
            else:
                return AppRunnerSource.DOCKER
        return AppRunnerSource.LOCAL

    @functools.cached_property
    def hash(self) -> str:
        config_type = self.type
        if config_type == AppRunnerSource.GITHUB:
            return git.remote_commit_hash(self.source, self.github_token)
        elif config_type == AppRunnerSource.DOCKER:
            return docker.image_hash(self.source)
        else:
            data = {
                "type": config_type,
                "source": self.source,
                "dependencies": self.dependencies,
                "pip_dependencies": self.pip_dependencies,
            }
            return objects.hash(data, hash_fn=hashlib.md5).hexdigest()

    @property
    def path(self) -> str:
        path = self.settings.get("path", None)
        if not path:
            path = "/".join(strs.tokenize(self._name)) if self._name else ""
        path = path.rstrip("/")
        if not path.startswith("/"):
            path = "/" + path
        return path

    @property
    def strip_path_prefix(self) -> bool:
        return parsers.to_bool(self.settings.get("strip_path_prefix", False))

    @property
    def source(self) -> str:
        return self.settings.get("source", None)

    @property
    def commands(self) -> list[str]:
        commands = list(self._iter("commands"))
        if len(commands) == 1:
            commands = shlex.split(commands[0])
        return commands

    @property
    def github_token(self) -> str:
        return self.settings.get("github_token", None)

    @property
    def dependencies(self) -> Iterable[str]:
        return self._iter("dependencies")

    @property
    def pip_dependencies(self) -> list[str]:
        return self._iter("pip_dependencies")

    @property
    def port(self) -> int:
        return int(self.settings.get(DATABRICKS_APP_PORT_ENV_VAR, 0))

    def has_dependency(self, dependency: str) -> bool:
        for dep in self.dependencies:
            if conda.dependency_name(dep) == dependency:
                return True
        return False

    def env(self, databricks: bool = False, os_environ: bool = False) -> dict:
        env = {}
        if os_environ:
            for name, value in os.environ.items():
                if not name.startswith(_APP_RUNNER_PREFIX):
                    env[name] = value
        if databricks:
            env[DATABRICKS_APP_PORT_ENV_VAR] = str(self.port)
            for name in DATABRICKS_APP_ENV_VARS:
                if value := self.settings.get(name, None):
                    env[name] = value
        env.update(self.settings.get("env", {}))
        return env

    def __repr__(self) -> str:
        out = {"name": self.name, "settings": self.settings}
        return f"{self.__class__.__name__}({out})"

    def _iter(self, name: str) -> Iterable[str]:
        value = self.settings.get(name, [])
        if not isinstance(value, str) and isinstance(value, Iterable):
            return value
        return [value] if value else []
