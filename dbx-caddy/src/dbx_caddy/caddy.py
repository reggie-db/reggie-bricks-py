import json
import os
import pathlib
import platform
import shutil
import stat
import subprocess
import tarfile
import tempfile
import threading
import urllib.request
import zipfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager

from lfp_logging import logs

"""
Install the Caddy binary for the current platform.

This module fetches the latest Caddy release metadata from GitHub, selects the
appropriate release asset for the current OS and CPU architecture, downloads the
archive, downloads and extracts it into a temporary directory, then installs the
`caddy` binary into `<home>/.local/bin`.
"""

LOG = logs.logger()

_CADDY_REPO = "caddyserver/caddy"
_LATEST_RELEASE_URL = f"https://api.github.com/repos/{_CADDY_REPO}/releases/latest"
_INSTALL_LOCK = threading.Lock()


def path(system_binary: bool = True) -> pathlib.Path:
    """Return a usable `caddy` binary path, installing if needed.

    Resolution order:
    - Prefer an existing system `caddy` discovered via `shutil.which("caddy")`.
    - Otherwise check the expected install location `<home>/.local/bin/<binary>`.
    - If not present, acquire a process-local lock and re-check both locations.
    - If still not present, install and return the installed path.
    """

    def _system_binary() -> pathlib.Path | None:
        if system_binary:
            if existing := shutil.which("caddy"):
                return pathlib.Path(existing)
        return None

    if bin_path := _system_binary():
        return bin_path

    bin_path = _install_path()
    if bin_path.exists():
        return bin_path

    with _INSTALL_LOCK:
        if bin_path := _system_binary():
            return bin_path

        bin_path = _install_path()
        if bin_path.exists():
            return bin_path

        return install(bin_path)


def install(dst: pathlib.Path | None = None) -> pathlib.Path:
    """Install Caddy into `<home>/.local/bin` and return the installed binary path.

    The installer:
    - Detects the current OS and CPU architecture.
    - Looks up the matching download asset from the latest GitHub release.
    - Downloads and extracts the archive in a temporary directory.
    - Moves the extracted `caddy` binary into `<home>/.local/bin`.
    - Ensures the extracted `caddy` binary is marked executable on POSIX systems.

    Args:
        dst: Optional destination path for the final installed binary. When omitted,
            the default computed by `_install_path()` is used.

    Returns:
        The path to the extracted `caddy` executable (`caddy` or `caddy.exe`).
    """

    dst = dst or _install_path()
    dst.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="dbx-caddy-") as td:
        tmp_dir = pathlib.Path(td)

        os_name, arch = _detect_platform()
        asset_name, asset_url = _find_latest_asset_url(os_name=os_name, arch=arch)

        archive_path = tmp_dir / asset_name
        _download_to_path(asset_url, archive_path)

        extracted_paths = _extract_archive(archive_path, tmp_dir)
        binary_path = _find_extracted_binary(tmp_dir, extracted_paths)

        _move_binary(binary_path, dst)
        _ensure_executable(dst)

        return dst


@contextmanager
def run(config: str | pathlib.Path, *args, **kwargs) -> Iterator[subprocess.Popen]:
    """Run `caddy run` with a provided config path or inline Caddyfile contents.

    Config resolution:
    - If `config` is a `pathlib.Path`, it is treated as a config file path.
    - If `config` is a string and it points to an existing file, it is treated as a
      config file path.
    - Otherwise, the string is treated as the contents of a config file and written
      to a temporary file for the duration of the context manager.

    Argument mapping:
    - Positional `args` are treated as flags. For example, `"watch"` becomes `--watch`.
      If an arg already starts with `-`, it is passed through unchanged.
    - Keyword `kwargs` are treated as `--key value` pairs. `snake_case` keys are
      converted to `kebab-case`. If a value is `True`, it is treated as a flag;
      if `False` or `None`, it is omitted.

    Example:
        `run("...caddyfile contents...", "watch")` runs:
        `caddy run --config <tempfile> --watch`
    """

    config_path, cleanup = _materialize_config(config)
    cmd = [str(path()), "run", "--config", str(config_path)]
    cmd.extend(_flags_from_args(args))
    cmd.extend(_flags_from_kwargs(kwargs))

    proc = subprocess.Popen(cmd)
    try:
        yield proc
    finally:
        _terminate_wait_kill(proc, timeout_seconds=10.0)
        if cleanup is not None:
            cleanup()


def _terminate_wait_kill(proc: subprocess.Popen, *, timeout_seconds: float) -> None:
    """Best-effort stop for a subprocess.

    If the process is still running, terminate it, wait up to `timeout_seconds`,
    and then kill it if needed.
    """

    if proc.poll() is not None:
        return

    try:
        proc.terminate()
    except Exception:
        LOG.exception("Failed to terminate caddy process")
        return

    try:
        proc.wait(timeout=timeout_seconds)
        return
    except subprocess.TimeoutExpired:
        pass

    try:
        proc.kill()
    except Exception:
        LOG.exception("Failed to kill caddy process")
        return

    try:
        proc.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        LOG.warning("caddy process did not exit after kill()")


def _install_path() -> pathlib.Path:
    """Return the expected final install path for this platform."""

    binary_name = "caddy.exe" if platform.system().lower() == "windows" else "caddy"
    return _local_bin_dir() / binary_name


def _materialize_config(
    config: str | pathlib.Path,
) -> tuple[pathlib.Path, Callable[[], None] | None]:
    """Return `(config_path, cleanup)` for `caddy run --config`.

    The cleanup callback (if returned) deletes the temporary config file.
    """

    if isinstance(config, pathlib.Path):
        return config, None

    config_str = str(config)
    p = pathlib.Path(config_str).expanduser()
    if p.exists() and p.is_file():
        return p, None

    fd, tmp_name = tempfile.mkstemp(prefix="dbx-caddy-", suffix=".caddyfile")
    tmp_path = pathlib.Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(config_str)
            f.flush()
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        finally:
            raise

    def _cleanup() -> None:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            LOG.exception("Failed to delete temp Caddy config %s", tmp_path)

    return tmp_path, _cleanup


def _flags_from_args(args: tuple[object, ...]) -> list[str]:
    """Convert positional args to `--flag` CLI args."""

    out: list[str] = []
    for a in args:
        if a is None:
            continue
        s = str(a).strip()
        if not s:
            continue
        if s.startswith("-"):
            out.append(s)
        else:
            out.append(f"--{s.replace('_', '-')}")
    return out


def _flags_from_kwargs(kwargs: dict[str, object]) -> list[str]:
    """Convert keyword args to `--key value` CLI args."""

    out: list[str] = []
    for k, v in kwargs.items():
        key = f"--{k.replace('_', '-')}"
        if v is True:
            out.append(key)
            continue
        if v is False or v is None:
            continue
        out.append(key)
        out.append(str(v))
    return out


def _detect_platform() -> tuple[str, str]:
    """Return `(os_name, arch)` matching Caddy release naming."""

    system = platform.system().lower()
    machine = platform.machine().lower()

    os_map = {
        "darwin": "mac",
        "linux": "linux",
        "windows": "windows",
        "freebsd": "freebsd",
    }
    if system not in os_map:
        raise ValueError(f"Unsupported OS for Caddy install: {platform.system()!r}")

    arch_map = {
        "x86_64": "amd64",
        "amd64": "amd64",
        "aarch64": "arm64",
        "arm64": "arm64",
        "armv5tel": "armv5",
        "armv5tejl": "armv5",
        "armv6l": "armv6",
        "armv7l": "armv7",
        "ppc64le": "ppc64le",
        "riscv64": "riscv64",
        "s390x": "s390x",
    }
    if machine not in arch_map:
        raise ValueError(
            f"Unsupported architecture for Caddy install: {platform.machine()!r}"
        )

    return os_map[system], arch_map[machine]


def _find_latest_asset_url(*, os_name: str, arch: str) -> tuple[str, str]:
    """Return `(asset_name, browser_download_url)` for the current platform."""

    release = _github_json(_LATEST_RELEASE_URL)
    assets = release.get("assets") or []

    # Asset names look like: caddy_2.10.2_linux_amd64.tar.gz (or windows_amd64.zip)
    # Also present are src/buildable-artifact assets which we intentionally ignore.
    suffixes = (".tar.gz", ".zip")
    candidates: list[tuple[str, str]] = []
    for asset in assets:
        name = asset.get("name")
        url = asset.get("browser_download_url")
        if not name or not url:
            continue
        if not name.startswith("caddy_"):
            continue
        if not name.endswith(suffixes):
            continue
        if f"_{os_name}_{arch}." not in name:
            continue
        candidates.append((name, url))

    if not candidates:
        tag = release.get("tag_name") or "<unknown>"
        raise ValueError(
            f"No matching Caddy asset found for os={os_name!r} arch={arch!r} (tag={tag!r})"
        )

    # There should be exactly one. If multiple exist, pick the shortest name (most specific).
    candidates.sort(key=lambda t: (len(t[0]), t[0]))
    return candidates[0]


def _github_json(url: str) -> dict:
    """Fetch JSON from GitHub with a minimal User-Agent."""

    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "dbx-caddy",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310
        data = resp.read()
    return json.loads(data.decode("utf-8"))


def _download_to_path(url: str, path: pathlib.Path) -> None:
    """Download `url` to `path` (overwriting if present)."""

    LOG.info("Downloading Caddy asset to %s", path)
    req = urllib.request.Request(url, headers={"User-Agent": "dbx-caddy"})
    with urllib.request.urlopen(req, timeout=120) as resp:  # noqa: S310
        with path.open("wb") as f:
            while True:
                chunk = resp.read(1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)


def _extract_archive(
    archive_path: pathlib.Path, dir: pathlib.Path
) -> list[pathlib.Path]:
    """Extract `archive_path` into `dir` and return extracted file paths."""

    if archive_path.name.endswith(".tar.gz"):
        return _extract_tar_gz(archive_path, dir)
    if archive_path.name.endswith(".zip"):
        return _extract_zip(archive_path, dir)
    raise ValueError(f"Unsupported archive type: {archive_path.name!r}")


def _extract_tar_gz(
    archive_path: pathlib.Path, dir: pathlib.Path
) -> list[pathlib.Path]:
    extracted: list[pathlib.Path] = []
    with tarfile.open(archive_path, mode="r:gz") as tf:
        members = tf.getmembers()
        _validate_archive_paths(
            [pathlib.Path(m.name) for m in members if m.name], target_dir=dir
        )
        tf.extractall(path=dir, filter="data")  # noqa: S202 - paths validated above
        for m in members:
            if m.name:
                extracted.append(dir / m.name)
    return extracted


def _extract_zip(archive_path: pathlib.Path, dir: pathlib.Path) -> list[pathlib.Path]:
    extracted: list[pathlib.Path] = []
    with zipfile.ZipFile(archive_path) as zf:
        names = [n for n in zf.namelist() if n]
        _validate_archive_paths([pathlib.Path(n) for n in names], target_dir=dir)
        zf.extractall(path=dir)
        for n in names:
            extracted.append(dir / n)
    return extracted


def _validate_archive_paths(
    paths: list[pathlib.Path], *, target_dir: pathlib.Path
) -> None:
    """Reject paths that would escape `target_dir` when extracted."""

    base = target_dir.resolve()
    for p in paths:
        # Treat all archive paths as relative.
        if p.is_absolute() or ".." in p.parts:
            raise ValueError(f"Unsafe archive member path: {str(p)!r}")
        resolved = (base / p).resolve()
        if base != resolved and base not in resolved.parents:
            raise ValueError(f"Unsafe archive member path: {str(p)!r}")


def _find_extracted_binary(
    dir: pathlib.Path, extracted_paths: list[pathlib.Path]
) -> pathlib.Path:
    """Locate the extracted `caddy` binary within the extracted files."""

    expected = "caddy.exe" if platform.system().lower() == "windows" else "caddy"

    direct = dir / expected
    if direct.exists():
        return direct

    for p in extracted_paths:
        if p.name == expected and p.exists():
            return p

    raise FileNotFoundError(
        f"Extracted archive did not contain expected binary {expected!r} under {str(dir)!r}"
    )


def _ensure_executable(binary_path: pathlib.Path) -> None:
    """Ensure `binary_path` is executable on POSIX systems."""

    if platform.system().lower() == "windows":
        return

    mode = os.stat(binary_path).st_mode
    os.chmod(binary_path, mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _local_bin_dir() -> pathlib.Path:
    """Determine `<home>/.local/bin` using the requested HOME fallbacks.

    Home resolution rules (in order):
    - Use `$HOME` if it is set and exists.
    - Otherwise use `/home` if it exists.
    - Otherwise create `/tmp/home` and use that.
    """

    home_env = (os.environ.get("HOME") or "").strip()
    if home_env:
        p = pathlib.Path(home_env)
        if p.exists():
            return p / ".local" / "bin"

    home_dir = pathlib.Path("/home")
    if home_dir.exists():
        return home_dir / ".local" / "bin"

    tmp_home = pathlib.Path("/tmp/home")
    tmp_home.mkdir(parents=True, exist_ok=True)
    return tmp_home / ".local" / "bin"


def _move_binary(src: pathlib.Path, dst: pathlib.Path) -> None:
    """Move `src` to `dst`, replacing `dst` if it exists."""

    LOG.info("Installing Caddy to %s", dst)
    dst.parent.mkdir(parents=True, exist_ok=True)

    if dst.exists():
        dst.unlink()
    shutil.move(str(src), str(dst))


if __name__ == "__main__":
    print(path(system_binary=True))
