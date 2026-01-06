import functools
import json
import logging
import re
import tempfile
from pathlib import Path
from typing import Any

import sh
from reggie_core import logs, objects, paths

from reggie_app_runner import conda

LOG = logs.logger("caddy")

_CONDA_ENV_NAME = "_caddy"
_CONDA_PACKAGE_NAME = "caddy"
_LOG_LEVEL_PATTERN = re.compile(r'"level"\s*:\s*"(\S+?)"', re.IGNORECASE)
_EXIT_CODE_PATTERN = re.compile(r'"exit_code"\s*:\s*(\d+)', re.IGNORECASE)


def command() -> sh.Command:
    """Return a baked ``sh.Command`` for the Caddy executable within our env."""
    return conda.run(_conda_env_name()).bake("caddy")


def run(config: Path | dict[str, Any] | str, *args, **kwargs) -> sh.RunningCommand:
    """Run Caddy with a provided config path, dict, or Caddyfile string.
    Creates a temporary config file when given a dict or string and streams logs, mapping JSON lines to appropriate log levels and triggering cleanup on exit codes.
    """
    config_file = _to_caddy_file(config)

    def _done(*_):
        # Best-effort cleanup of the temp config file
        try:
            config_file.unlink()
            LOG.info(f"Deleted config file {config_file}")
        except FileNotFoundError:
            pass

    def _out(error, line):
        # Derive log level from stream or JSON payload; detect exit codes for completion
        levelno = logging.ERROR if error else logging.INFO
        line = line.rstrip()
        if line:
            for match in _LOG_LEVEL_PATTERN.finditer(line):
                line_levelno, _ = logs.get_level(match.group(1))
                if line_levelno:
                    levelno = line_levelno
                    break
            for match in _EXIT_CODE_PATTERN.finditer(line):
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(msg, dict) and msg.get("exit_code") is not None:
                    _done()

        LOG.log(levelno, line)

    run_args = ["run", "--config", config_file]
    run_args.extend(args)
    # Launch Caddy with the materialized config file; wire stdout/stderr/done hooks
    proc = command()(
        *run_args,
        _out=lambda x: _out(False, x),
        _err=lambda x: _out(True, x),
        _done=_done,
        **kwargs,
    )

    return proc


@functools.cache
def _conda_env_name():
    """Ensure the Caddy env is present and return the environment name."""
    conda.update(_CONDA_ENV_NAME, _CONDA_PACKAGE_NAME)
    return _CONDA_ENV_NAME


def _to_caddy_file(config: str | Path | dict[str, Any]) -> Path:
    """Materialize a Caddy configuration to a file and return its path."""
    if isinstance(config, Path):
        return config
    elif isinstance(config, dict):
        # Serialize dict configs to JSON content
        config_content = objects.to_json(config, indent=2)
        config_extension = "json"
    else:
        config = str(config)
        # Treat input as a filesystem path when it exists
        if path := paths.path(config, exists=True):
            return path
        # Otherwise assume a raw Caddyfile string
        config_content = config
        config_extension = "caddyfile"
    with tempfile.NamedTemporaryFile(
        mode="w+", suffix=f".{config_extension}", delete=False
    ) as caddy_file:
        # Persist content to a temp file and return its absolute path
        caddy_file.write(config_content)
        caddy_file.flush()
        return paths.path(caddy_file.name, absolute=True)
