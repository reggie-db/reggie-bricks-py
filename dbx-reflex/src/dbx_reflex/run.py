import argparse
import logging
import os
import signal
import socket
import subprocess
import sys
import time

from dbx_caddy import caddy

LOG = logging.getLogger(__name__)
_DEFAULT_APP_PORT = 8000


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="dbx-reflex-run",
        description="Run a Reflex app behind Caddy with HMR proxy support.",
    )
    parser.add_argument("--backend-port", type=int, default=None)
    parser.add_argument("--frontend-port", type=int, default=None)
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


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def _resolve_ports(args: argparse.Namespace) -> tuple[int, int, int]:
    app_port = args.app_port or _env_int("DATABRICKS_APP_PORT", _DEFAULT_APP_PORT)
    backend_port = args.backend_port or _env_int("REFLEX_BACKEND_PORT", 0)
    frontend_port = args.frontend_port or _env_int("REFLEX_FRONTEND_PORT", 0)
    if backend_port <= 0:
        backend_port = _get_free_port()
    if frontend_port <= 0:
        frontend_port = _get_free_port()
    if frontend_port == backend_port:
        frontend_port = _get_free_port()
    return app_port, backend_port, frontend_port


def _caddyfile(app_port: int, backend_port: int, frontend_port: int) -> str:
    return (
        f":{app_port} {{\n"
        f"    handle /_hmr {{\n"
        f"        reverse_proxy localhost:{frontend_port}\n"
        f"    }}\n\n"
        f"    handle /_upload {{\n"
        f"        reverse_proxy localhost:{backend_port}\n"
        f"    }}\n\n"
        f"    handle /_event {{\n"
        f"        reverse_proxy localhost:{backend_port}\n"
        f"    }}\n\n"
        f"    @websockets {{\n"
        f"        header Connection *Upgrade*\n"
        f"        header Upgrade websocket\n"
        f"    }}\n\n"
        f"    reverse_proxy @websockets localhost:{backend_port}\n"
        f"    reverse_proxy localhost:{frontend_port}\n"
        f"}}\n"
    )


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
    app_port, backend_port, frontend_port = _resolve_ports(args)
    reflex_args = _normalize_reflex_args(args.reflex_args)
    env = dict(os.environ)
    env["DATABRICKS_APP_PORT"] = str(app_port)
    env["REFLEX_BACKEND_PORT"] = str(backend_port)
    env["REFLEX_FRONTEND_PORT"] = str(frontend_port)
    LOG.info(
        "Resolved ports: app=%s backend=%s frontend=%s",
        app_port,
        backend_port,
        frontend_port,
    )

    proc_holder: dict[str, list[subprocess.Popen]] = {}

    def _handle_signal(signum, _frame):
        LOG.info("Received signal %s", signum)
        if procs := proc_holder.get("reflex"):
            _stop_all(procs)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    with caddy.run(
        _caddyfile(app_port, backend_port, frontend_port), "watch"
    ) as caddy_proc:
        reflex_proc = _start_reflex(reflex_args=reflex_args, env=env)
        reflex_processes = [reflex_proc]
        proc_holder["reflex"] = reflex_processes
        try:
            idx, exit_code = _wait_until_any_exits([reflex_proc, caddy_proc])
            if idx == 0:
                LOG.error("Reflex exited with code %s", exit_code)
            else:
                LOG.error("Caddy exited with code %s", exit_code)
            return exit_code
        finally:
            _stop_all(reflex_processes)


if __name__ == "__main__":
    raise SystemExit(main())
