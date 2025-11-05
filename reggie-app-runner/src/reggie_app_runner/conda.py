import asyncio
import functools
import logging
import os
import platform
import re
import subprocess
from collections.abc import Iterable
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.request import urlretrieve

import sh
import yaml
from filelock import FileLock
from reggie_core import logs, paths

from reggie_app_runner import docker

_CONDA_DIR_NAME = ".miniforge3"
_CONDA_DEPENDENCY_PATTERN = re.compile(
    r"^(?:[A-Za-z0-9_.-]+::)?(?P<name>[A-Za-z0-9_.-]+)"
)


@functools.cache
def exec() -> Path:
    """Return the conda executable path. Install Miniforge to ~/.miniforge3 if needed."""
    conda_dir = paths.home() / _CONDA_DIR_NAME
    bin_path = conda_dir / "bin" / "conda"

    def _bin_path_valid():
        return bin_path.is_file() and os.access(bin_path, os.X_OK)

    if not _bin_path_valid():
        installer_dir = _install_conda(conda_dir)
        if not _bin_path_valid():
            raise ValueError(
                f"Failed to install conda - path:{bin_path} installer_dir:{installer_dir}"
            )
    logging.getLogger().info("Conda executable path: %s", bin_path)
    return bin_path


def run(env_name: str) -> sh.Command:
    return sh.Command(exec()).bake(
        "run",
        "-n",
        env_name,
        "--no-capture-output",
        _arg_preprocess=lambda a, k: _run_arg_preprocess(env_name, a, k),
    )


def update(
    env_name: str,
    *dependencies: str,
    pip_dependencies: Iterable[str] = None,
):
    """Create a conda environment with the given name and dependencies."""
    dependencies = list(dependencies)
    if pip_dependencies:
        dependencies.append({"pip": pip_dependencies})
    if not dependencies or (
        pip_dependencies and not any(d == "pip" for d in dependencies)
    ):
        dependencies.append("pip")
    conda_env = {
        "name": env_name,
        "dependencies": dependencies,
    }

    with NamedTemporaryFile(mode="w", suffix=".yml") as f:
        conda_env_content = yaml.dump(conda_env)
        f.write(conda_env_content)
        f.flush()

        def _log_msg(ran, call_args, pid=None):
            ran = ran.replace(str(exec()), "conda")
            return f"{ran}, pid {pid}"

        sh.Command(exec())("env", "update", "-f", f.name, "--prune", _log_msg=_log_msg)


def dependency_name(dependency: str) -> str:
    if dependency:
        match = _CONDA_DEPENDENCY_PATTERN.match(dependency)
        if match:
            return match.group("name")
    return None


def _install_conda(dir: Path) -> Path:
    """Download and run the Miniforge installer into the given directory, cached by URL."""
    url = _install_url()
    log = logs.logger("conda_install")
    installer_file_name = "installer.sh"
    installer_path = dir / installer_file_name

    def _installer_path_valid():
        return installer_path.is_file() and os.access(installer_path, os.X_OK)

    installer_lock_path = dir / f"{installer_file_name}.lock"
    if not _installer_path_valid():
        with FileLock(installer_lock_path):
            if not _installer_path_valid():
                log.info(f"Downloading Conda - url:{url} path:{installer_path}")
                urlretrieve(url, installer_path)
                installer_path.chmod(0o755)
                log.info(f"Installing Conda - path:{installer_path}")
                subprocess.run([installer_path, "-b", "-p", dir], check=True, text=True)

    def _run_installer(path: Path):
        installer_path = path / "installer.sh"
        log.info(f"Downloading Conda - url:{url} path:{installer_path}")
        urlretrieve(url, installer_path)
        installer_path.chmod(0o755)
        log.info(f"Installing Conda - path:{installer_path}")
        subprocess.run([installer_path, "-b", "-p", dir], check=True, text=True)

    return paths.cache_store(url + "v4", _run_installer)


def _install_url() -> str:
    """Construct the Miniforge installer URL for the current platform and arch."""
    sysname = platform.system()
    arch = platform.machine()

    arch_map = {
        "x86_64": "amd64",
        "aarch64": "arm64",
    }

    for k, v in arch_map.items():
        if k.casefold() == arch.casefold():
            arch = v
            break

    os_map = {
        "MacOSX": "Darwin",
    }

    for k, v in os_map.items():
        if k.casefold() == sysname.casefold():
            sysname = v
            break

    return f"https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-{sysname}-{arch}.sh"


def _run_arg_preprocess(env_name: str, args, kwargs):
    if "_log_msg" not in kwargs:
        exec_path = exec()
        exec_name = exec_path.stem

        def _log_msg(ran, _, pid=None):
            ran = ran.replace(str(exec()), exec_name)
            return f"{ran}, pid {pid}"

        kwargs["_log_msg"] = _log_msg

    stdout_log_levelno = (
        logging.INFO if (kwargs.get("_bg", False) and "_out" not in kwargs) else None
    )
    stderr_log_levelno = logging.WARNING if "_err" not in kwargs else None
    if any(k is not None for k in [stdout_log_levelno, stderr_log_levelno]):
        log = logs.logger("conda_run")

        def _out(levelno, line, queue, process):
            if line.endswith("\n"):
                line = line[:-1]
            log.log(levelno, f"{env_name} | {line}")

        if stdout_log_levelno is not None:
            kwargs["_out"] = lambda line, queue, process: _out(
                stdout_log_levelno, line, queue, process
            )
        if stderr_log_levelno is not None:
            kwargs["_err"] = lambda line, queue, process: _out(
                stderr_log_levelno, line, queue, process
            )
    return args, kwargs


async def main():
    print(dependency_name("caddy"))
    print(dependency_name("caddy::caddy"))
    print(dependency_name("cool::caddy=12"))
    deps = [
        "caddy",
        "curl",
    ]
    if not docker.path():
        deps.append("udocker")
    update("cool", *deps)
    run("cool")("caddy", "--version", _bg=True).wait()
    run("cool")(
        docker.path() or "udocker",
        "run",
        "hello-world",
        _bg=True,
    ).wait()
    update("cool", "openjdk", pip_dependencies=["requests"])
    run("cool")("java", "--version", _bg=True).wait()


if __name__ == "__main__":
    logging.getLogger().info("test")
    asyncio.run(main())

    # conda = path()
    # print(json.dumps(env(include_os=False), indent=2))
    # install("caddy", "curl", "openjdk")
    # for commands in [
    #     ["conda", "info"],
    #     ["caddy", "version"],
    #     ["curl", "--version"],
    #     ["java", "--version"],
    #     ["bash", "-c", "echo $JAVA_HOME"],
    # ]:
    #     subprocess.run(commands, env=env(), check=True)

    # print(f"Conda executable: {conda}")
    # start(["conda", "info"]).wait()
