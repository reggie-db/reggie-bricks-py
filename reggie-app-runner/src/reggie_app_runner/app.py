import asyncio
import json
import os
import shutil
import signal
import socket
from pathlib import Path
from typing import Any, Sequence

import psutil
import sh
from reggie_core import logs, objects, paths, strs
from sh import RunningCommand

from reggie_app_runner import app_runner, caddy, conda, docker, git
from reggie_app_runner.app_runner import AppRunnerConfig, AppRunnerSource

LOG = logs.logger(__file__)


def run():
    configs = list(app_runner.read_configs())
    procs: list[RunningCommand] = []
    try:
        for config in configs:
            app_dir = _app_dir(config)
            proc = start(app_dir, config)
            if not proc.is_alive():
                raise RuntimeError(
                    f"failed to start process - pid:{proc.pid} name:{config.name}"
                )
            procs.append(proc)
        caddy_proc = _caddy_run(configs)
        procs.append(caddy_proc)
        LOG.info(f"all processes started: {list(p.pid for p in procs)}")
        caddy_proc.wait()
    finally:
        for proc in procs:
            LOG.info(f"shutting down process {proc.pid}")
            _shutdown_process(proc)


def start(app_dir: Path, config: AppRunnerConfig) -> RunningCommand:
    source_dir = prepare(app_dir, config)

    def _build_env(env: dict):
        home_dir = app_dir / "home"
        home_dir.mkdir(parents=True, exist_ok=True)
        env["HOME"] = str(home_dir)
        return env

    if config.type == AppRunnerSource.DOCKER:
        docker_env = _build_env(config.env(databricks=True))
        env_args = []
        for key, value in docker_env.items():
            env_args.append("-e")
            env_args.append(f'{key}="{value.replace('"', '\\"')}"')
        docker_commands = ["run"]
        if docker.runtime_path():
            docker_commands = docker_commands + [
                "--rm",
                "-p",
                f"{config.port}:{config.port}",
            ]
        docker_commands = docker_commands + env_args + [config.source]
        proc_env = os.environ.copy()
        command = docker.command().bake(*docker_commands)
    else:
        env_name = f"app_{config.name}"
        conda.update(
            env_name, *config.dependencies, pip_dependencies=config.pip_dependencies
        )
        proc_env = _build_env(config.env(databricks=True, os_environ=True))
        command = conda.run(env_name)

    proc = command(
        *config.commands,
        _bg=True,
        _bg_exc=False,
        _cwd=source_dir,
        _env=proc_env,
    )
    return proc


def prepare(app_dir: Path, config: AppRunnerConfig) -> Path:
    source_dir = app_dir / f"source_{config.type}_{config.hash}"
    if source_dir.exists():
        shutil.rmtree(source_dir)
    source_dir.mkdir(parents=True, exist_ok=True)
    if config.type == AppRunnerSource.GITHUB:
        git.clone(config.source, source_dir, token=config.github_token)
    elif config.type == AppRunnerSource.DOCKER:
        docker.pull(config.source)
        pass
    return source_dir


def _app_dir(config: AppRunnerConfig):
    dir_name = "app_runner"
    if not os.environ.get("DATABRICKS_WORKSPACE_ID"):
        dir_name = f".{dir_name}"
    root_dir = paths.home() / dir_name
    root_dir.mkdir(parents=True, exist_ok=True)
    return root_dir / config.name


def _caddy_run(configs: Sequence[AppRunnerConfig]) -> RunningCommand:
    caddy_listen_port = int(os.environ[app_runner.DATABRICKS_APP_PORT_ENV_VAR])
    LOG.info(f"caddy_listen_port: {caddy_listen_port}")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.5)
    try:
        sock.connect(("localhost", caddy_listen_port))
        raise RuntimeError(f"caddy_listen_port in use:{caddy_listen_port}")
    except OSError:
        pass
    finally:
        sock.close()
    caddy_log_level = None
    caddy_config = _build_caddy_config(
        caddy_listen_port, configs, log_level=caddy_log_level
    )
    LOG.info(f"caddy_config: {objects.to_json(caddy_config)}")
    return caddy.run(caddy_config, _bg=True, _bg_exc=False)


def _build_caddy_config(
    listen_port: int, configs: Sequence[AppRunnerConfig], log_level: Any | None = None
) -> dict[str, Any]:
    routes = []
    configs = sorted(configs, key=lambda x: len(x.path) * -1)
    for config in configs:
        handle = []
        config_path = config.path
        if not config_path or "/" == config_path:
            config_path = None
        if config_path and config.strip_path_prefix:
            handle.append(
                {"handler": "rewrite", "strip_path_prefix": config.path},
            )
        handle.append(
            {
                "handler": "reverse_proxy",
                "upstreams": [{"dial": f"localhost:{config.port}"}],
            },
        )
        route = {
            "handle": handle,
        }
        if config_path:
            route["match"] = [{"path": [f"{config_path}*"]}]
        routes.append(route)

    server = {
        "listen": [f":{listen_port}"],
        "routes": routes,
    }
    if log_level is not None:
        _, log_level_name = logs.get_level(log_level)
    else:
        log_level_name = None
    if log_level_name:
        server["logs"] = {"default_logger_name": "default"}
    config = {
        "admin": {"disabled": True, "config": {"persist": False}},
        "apps": {"http": {"servers": {"srv0": server}}},
    }
    if log_level_name:
        config["logging"] = {
            "logs": {
                "default": {
                    "level": log_level_name.upper(),
                    "writer": {"output": "stdout"},
                    "encoder": {"format": "json"},
                    "include": ["http.log.access"],
                }
            }
        }
    return config


def _shutdown_process(
    pid_or_proc: int | sh.RunningCommand,
    timeout: float = 10.0,
    kill_children: bool = True,
):
    # Resolve pid and psutil.Process
    if isinstance(pid_or_proc, int):
        pid = pid_or_proc
    else:
        pid = pid_or_proc.pid
    try:
        proc = psutil.Process(pid)
    except psutil.NoSuchProcess:
        LOG.info(f"process not found {pid}")
        return
    # Compute groups
    try:
        target_pgid = os.getpgid(pid)
    except ProcessLookupError:
        LOG.info(f"process not found {pid}")
        return
    python_pgid = os.getpgrp()
    LOG.info(
        f"terminating pid={pid}, target_pgid={target_pgid}, python_pgid={python_pgid}"
    )
    # Optionally collect children first to handle mixed groups
    children = []
    if kill_children:
        try:
            children.extend(proc.children(recursive=True))
        except psutil.Error:
            pass

    def _signal(sig: int):
        sent = False
        # If the child is in a different group than us, prefer a group signal
        if target_pgid != python_pgid:
            try:
                os.killpg(target_pgid, sig)
                sent = True
                LOG.info(f"sent {signal.Signals(sig).name} to pgid {target_pgid}")
            except ProcessLookupError:
                pass
            except PermissionError as e:
                LOG.info(f"killpg permission error: {e}. Falling back to pid")
        # Always also target the main pid in case it changed groups or already exited
        try:
            os.kill(pid, sig)
            sent = True
            LOG.info(f"sent {signal.Signals(sig).name} to pid {pid}")
        except ProcessLookupError:
            pass
        # Fan out to any discovered children that may live outside the pgid
        for ch in children:
            try:
                os.kill(ch.pid, sig)
            except ProcessLookupError:
                pass
        return sent

    # TERM then wait
    if _signal(signal.SIGTERM):
        try:
            proc.wait(timeout=timeout)
        except psutil.TimeoutExpired:
            pass
        except psutil.NoSuchProcess:
            return
    # KILL then wait
    if _signal(signal.SIGKILL):
        try:
            proc.wait(timeout=timeout)
        except psutil.TimeoutExpired:
            LOG.info(
                f"pid {pid} did not exit after {signal.Signals(signal.SIGKILL).name}"
            )
        except psutil.NoSuchProcess:
            pass
    # Final sweep for children
    for ch in children:
        try:
            ch.wait(timeout=0.1)
        except psutil.NoSuchProcess:
            pass
        except psutil.TimeoutExpired:
            try:
                os.kill(ch.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


if __name__ == "__main__":
    logs.auto_config()
    logs.logger().warning("info")
    os.environ["DATABRICKS_APP_PORT"] = "8000"
    os.environ["DATABRICKS_APP_NAME"] = "reggie_guy"
    os.environ["DATABRICKS_APP_RUNNER__DEFAULT__GITHUB_TOKEN"] = "default_token"
    os.environ["TEST"] = "root_replaced"
    if True:
        os.environ["DATABRICKS_APP_RUNNER_TEST"] = "nested_replaced"
        os.environ["DATABRICKS_APP_RUNNER_ENV__PYTHONUNBUFFERED"] = (
            "@format {this.test}"
        )
        os.environ["DATABRICKS_APP_RUNNER_DEPENDENCIES"] = "ttyd"
        os.environ["DATABRICKS_APP_RUNNER_STRIP_PATH_PREFIX"] = "true"
        os.environ["DATABRICKS_APP_RUNNER_SOURCE"] = ""
        # os.environ["DATABRICKS_APP_RUNNER_PATH"] = "/ttyd"
        shell = "bash"
        commands = [
            "ttyd",
            "-p",
            "@format {this.databricks_app_port}",
            "-i",
            "0.0.0.0",
            "--writable",
            shell,
        ]
        os.environ["DATABRICKS_APP_RUNNER_COMMANDS"] = f"@json {json.dumps(commands)}"
    for name in ["APP_FUN", "APP_ABC_COOL0_1"]:
        tokenized_name = "_".join(strs.tokenize(name))
        os.environ[f"DATABRICKS_APP_RUNNER__{name}__SOURCE"] = "ealen/echo-server"
        os.environ[f"DATABRICKS_APP_RUNNER__{name}__COMMANDS"] = ""
        os.environ[f"DATABRICKS_APP_RUNNER__{name}__TEST"] = "app_replaced"
        env = {"test": "test", "PORT": "@format {this.databricks_app_port}"}
        os.environ[f"DATABRICKS_APP_RUNNER__{name}__ENV"] = f"@json {json.dumps(env)}"
    asyncio.run(run())
