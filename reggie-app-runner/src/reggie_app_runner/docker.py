import functools
import hashlib
import json
import platform
import shutil

import sh
from reggie_core import paths

from reggie_app_runner import conda

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
    # Query the registry using a transport qualified image reference
    output = _skopeo()(
        "inspect",
        f"--override-os={image_os}",
        f"--override-arch={image_arch}",
        f"docker://{image_name}",
    )
    return json.loads(output)
