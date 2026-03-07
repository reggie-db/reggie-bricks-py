import argparse
import logging
import os
import signal
import subprocess
import sys
import time

LOG = logging.getLogger(__name__)
_DEFAULT_APP_PORT = 8000


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="dbx-reflex-run",
        description="Run a Reflex app in single-port mode.",
    )
    parser.add_argument("--app-port", type=int, default=None)
    parser.add_argument("reflex_args", nargs=argparse.REMAINDER)
    return parser.parse_args(argv)


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name, "")
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _resolve_app_port(args: argparse.Namespace) -> int:
    """Resolve app port from CLI first, then Databricks app environment."""
    return args.app_port or _env_int("DATABRICKS_APP_PORT", _DEFAULT_APP_PORT)


def _default_reflex_args() -> list[str]:
    return ["--env", "dev"]


def _normalize_reflex_args(raw_args: list[str]) -> list[str]:
    args = list(raw_args)
    if args and args[0] == "--":
        args = args[1:]
    if not args:
        return _default_reflex_args()
    return args


def _start_reflex(reflex_args: list[str], env: dict[str, str]) -> subprocess.Popen:
    cmd = ["reflex", "run", *reflex_args]
    LOG.info("Starting Reflex: %s", " ".join(cmd))
    if os.name == "posix":
        return subprocess.Popen(cmd, env=env, preexec_fn=os.setsid)
    return subprocess.Popen(cmd, env=env)


def _stop_process(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    try:
        if os.name == "posix":
            os.killpg(proc.pid, signal.SIGTERM)
        else:
            proc.terminate()
    except ProcessLookupError:
        return
    except Exception as exc:
        raise RuntimeError("Failed to terminate Reflex process") from exc
    try:
        proc.wait(timeout=5)
        return
    except subprocess.TimeoutExpired:
        LOG.warning("Reflex did not exit after SIGTERM, forcing kill")
    try:
        if os.name == "posix":
            os.killpg(proc.pid, signal.SIGKILL)
        else:
            proc.kill()
    except ProcessLookupError:
        return
    except Exception as exc:
        raise RuntimeError("Failed to kill Reflex process") from exc
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("Reflex process still running after SIGKILL") from exc


def _stop_all(processes: list[subprocess.Popen]) -> None:
    stop_errors: list[Exception] = []
    for proc in processes:
        try:
            _stop_process(proc)
        except Exception as stop_error:
            stop_errors.append(stop_error)
    if stop_errors:
        raise ExceptionGroup("Failed to stop one or more Reflex processes", stop_errors)


def _wait_until_any_exits(processes: list[subprocess.Popen]) -> tuple[int, int]:
    while True:
        for idx, proc in enumerate(processes):
            code = proc.poll()
            if code is not None:
                return idx, code
        time.sleep(0.1)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    app_port = _resolve_app_port(args)
    reflex_args = _normalize_reflex_args(args.reflex_args)
    env = dict(os.environ)
    env["DATABRICKS_APP_PORT"] = str(app_port)
    # Ensure Reflex uses single-port mode with no stale split-port overrides.
    env.pop("REFLEX_BACKEND_PORT", None)
    env.pop("REFLEX_FRONTEND_PORT", None)
    LOG.info("Resolved app port: %s", app_port)

    proc_holder: dict[str, list[subprocess.Popen]] = {}

    def _handle_signal(signum, _frame):
        LOG.info("Received signal %s", signum)
        if procs := proc_holder.get("reflex"):
            _stop_all(procs)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    reflex_proc = _start_reflex(reflex_args=reflex_args, env=env)
    reflex_processes = [reflex_proc]
    proc_holder["reflex"] = reflex_processes
    try:
        _, exit_code = _wait_until_any_exits(reflex_processes)
        LOG.error("Reflex exited with code %s", exit_code)
        return exit_code
    finally:
        _stop_all(reflex_processes)


if __name__ == "__main__":
    raise SystemExit(main())
