import functools
import hashlib
import json
import platform
import shutil

import sh
from lfp_logging import logs
from reggie_core import paths

from reggie_app_runner import conda

LOG = logs.logger()

_CONDA_ENV_NAME = "_docker"
_UDOCKER_NAME = "udocker"


@functools.cache
def runtime_path():
    """Return the path to a container runtime (docker or podman) if available."""
    for name in ["docker", "podman"]:
        if path := shutil.which(name):
            return paths.path(path, absolute=True)
    return None


def command():
    """Return a baked command for the runtime or fallback to ``udocker``."""
    command = runtime_path()
    if command:
        return sh.Command(command)
    return conda.run(_conda_env_name()).bake(_UDOCKER_NAME)


def image_hash(image_name: str):
    """Return a stable image hash using registry digest or normalized inspect data."""
    data = _inspect(image_name)
    digest = data.get("Digest", None)
    if digest:
        return digest
    # Normalize inspect JSON and hash when digest is not available
    dumped_data = json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(dumped_data).hexdigest()


def pull(image_name: str):
    """Pull an image using the available runtime, waiting for completion."""
    command()("pull", image_name, _bg=True).wait()


@functools.cache
def _conda_env_name():
    """Ensure the runtime helper env is present and return its name."""
    dependencies = ["skopeo"]
    pip_dependencies = []
    linux = platform.system().casefold() == "linux"
    (dependencies if linux else pip_dependencies).append(_UDOCKER_NAME)
    conda.update(
        _CONDA_ENV_NAME,
        *dependencies,
        pip_dependencies=pip_dependencies,
    )
    return _CONDA_ENV_NAME


def _skopeo():
    """Return a baked ``skopeo`` command within the managed environment."""
    return conda.run(_conda_env_name()).bake("skopeo")


def _inspect(image_name: str):
    """Inspect an image via skopeo using consistent OS/arch flags and return JSON."""
    image_os = "linux"
    image_arch = (
        "amd64" if platform.machine().lower() in ("arm64", "aarch64") else "amd64"
    )
    image_names = [image_name]
    image_name_mirror = _image_name_mirror(image_name)
    if image_name_mirror != image_name:
        image_names.append(image_name_mirror)
    # Query the registry using a transport qualified image reference
    error = None
    for inspect_image_name in image_names:
        output = _skopeo()(
            "inspect",
            f"--override-os={image_os}",
            f"--override-arch={image_arch}",
            f"docker://{inspect_image_name}",
        )
        try:
            return json.loads(output)
        except Exception as e:
            error = e
    raise error


def _image_name_mirror(image_name: str):
    image_registry = image_name.split("/", 1)[0]
    if not (
        "." in image_registry or ":" in image_registry or image_registry == "localhost"
    ):
        image_name = f"mirror.gcr.io/library/{image_name}"
    return image_name


if __name__ == "__main__":
    LOG.info(f"Runtime path: {runtime_path()}")
    LOG.info(f"Command: {command()}")
    LOG.info(f"Image hash: {image_hash('plexinc/pms-docker')}")
    LOG.info(
        f"Image hash (with tag): {image_hash('plexinc/pms-docker:1.42.2.10156-f737b826c')}"
    )
    pull("plexinc/pms-docker")
    LOG.info(f"Version check: {command()('version', _bg=True).wait()}")
    LOG.info(f"Hello world: {command()('run', 'hello-world', _bg=True).wait()}")
    LOG.info(f"Echo server: {command()('run', 'ealen/echo-server', _bg=True).wait()}")
