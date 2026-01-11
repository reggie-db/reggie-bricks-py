import asyncio
import hashlib
import json
import os
import shutil
import signal
import socket
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

import psutil
import sh
from lfp_logging import logs
from reggie_core import objects, paths
from sh import RunningCommand

from reggie_app_runner import app_runner, caddy, conda, docker, git
from reggie_app_runner.app_runner import AppRunnerConfig, AppRunnerSource

LOG = logs.logger()


async def run():
    configs = list(app_runner.read_configs())
    procs: list[RunningCommand] = []
    try:
        start_tasks = []
        for config in configs:
            app_dir = _app_dir(config)
            start_tasks.append(asyncio.create_task(start(app_dir, config)))
        results = await asyncio.gather(*start_tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, RunningCommand):
                procs.append(result)
        for result in results:
            if isinstance(result, Exception):
                raise result
        caddy_proc = _caddy_run(configs)
        procs.append(caddy_proc)
        LOG.info(f"all processes started: {list(p.pid for p in procs)}")
        await caddy_proc
    finally:
        for proc in procs:
            LOG.info(f"shutting down process {proc.pid}")
            _shutdown_process(proc)


async def start(app_dir: Path, config: AppRunnerConfig) -> RunningCommand:
    source_dir = await asyncio.to_thread(prepare, app_dir, config)
    env_name = f"app_{config.name}"
    await asyncio.to_thread(
        conda.update,
        env_name,
        *_dependencies(config),
        pip_dependencies=config.pip_dependencies,
    )

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
            env_args.append(f"{key}={value}")
        docker_commands = _commands(config)
        if docker.runtime_path():
            docker_commands = docker_commands + [
                "--rm",
                "-p",
                f"{config.port}:{config.port}",
            ]
        docker_commands = docker_commands + env_args + [config.source]
        proc_env = os.environ.copy()
        command = conda.run(env_name).bake(docker.command().bake(*docker_commands))
    else:
        proc_env = config.env(databricks=True, os_environ=True)
        command = conda.run(env_name).bake(*_commands(config))
    proc = command(
        _bg=True,
        _bg_exc=False,
        _cwd=source_dir,
        _env=proc_env,
        _async=True,
    )
    return proc


def prepare(app_dir: Path, config: AppRunnerConfig) -> Path:
    source_dir = app_dir / f"source_{config.type.name}_{config.hash}"
    if source_dir.exists():
        shutil.rmtree(source_dir)
    source_dir.mkdir(parents=True, exist_ok=True)
    if config.type == AppRunnerSource.GITHUB:
        git.clone(config.source, source_dir, token=config.github_token)
    elif config.type == AppRunnerSource.DOCKER:
        _docker_pull(config)
    return source_dir


def _app_dir(config: AppRunnerConfig):
    dir_name = "app_runner"
    if not os.environ.get("DATABRICKS_WORKSPACE_ID"):
        dir_name = f".{dir_name}"
    root_dir = paths.home() / dir_name
    root_dir.mkdir(parents=True, exist_ok=True)
    return root_dir / config.name


def _commands(config: AppRunnerConfig) -> list[str]:
    commands = config.commands
    if commands and commands[0].endswith(".py"):
        commands = ["uv", "run", *commands]
    return commands


def _dependencies(config: AppRunnerConfig) -> list[str]:
    commands = _commands(config)
    dependencies = config.dependencies
    if commands and commands[0] == "uv" and not config.has_dependency("uv"):
        dependencies = ["uv", *dependencies]
    return dependencies


def _docker_pull(config: AppRunnerConfig, force: bool = False) -> RunningCommand:
    last_pull = (
        _app_dir(config)
        / f".last_pull_{objects.hash(config.source, hash_fn=hashlib.md5)}"
    )
    if not force and last_pull.is_file():
        last_pull_time = last_pull.read_text()
        if (
            last_pull_time
            and last_pull_time.isdigit
            and int(last_pull_time) > datetime.now() - datetime.timedelta(hours=1)
        ):
            return
    docker.pull(config.source)
    last_pull.write_text(str(int(datetime.now().timestamp())))


def _caddy_run(configs: Sequence[AppRunnerConfig]) -> RunningCommand:
    caddy_listen_port = int(
        os.environ.get(app_runner.DATABRICKS_APP_PORT_ENV_VAR, 8000)
    )
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
    return caddy.run(caddy_config, _bg=True, _bg_exc=False, _async=True)


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
        log_level_name = logs.log_level(log_level)[0]
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
    # logs.auto_config()
    logs.logger().warning("info")
    os.environ["DATABRICKS_APP_PORT"] = "8000"
    os.environ["DATABRICKS_APP_NAME"] = "reggie_guy"
    os.environ["DATABRICKS_APP_RUNNER__DEFAULT__GITHUB_TOKEN"] = "default_token"
    os.environ["TEST"] = "root_replaced"
    for name in ["TEST_123"]:
        os.environ[f"DATABRICKS_APP_RUNNER__{name}__SOURCE"] = (
            "https://github.com/reggie-db/test-app"
        )
        os.environ[f"DATABRICKS_APP_RUNNER__{name}__COMMANDS"] = "app.py cool"
        env = {"PORT": "@format {this.databricks_app_port}"}
        os.environ[f"DATABRICKS_APP_RUNNER__{name}__ENV"] = f"@json {json.dumps(env)}"
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
        os.environ[f"DATABRICKS_APP_RUNNER__{name}__SOURCE"] = "ealen/echo-server"
        os.environ[f"DATABRICKS_APP_RUNNER__{name}__COMMANDS"] = ""
        os.environ[f"DATABRICKS_APP_RUNNER__{name}__TEST"] = "app_replaced"
        env = {"test": "test", "PORT": "@format {this.databricks_app_port}"}
        os.environ[f"DATABRICKS_APP_RUNNER__{name}__ENV"] = f"@json {json.dumps(env)}"

    asyncio.run(run())
