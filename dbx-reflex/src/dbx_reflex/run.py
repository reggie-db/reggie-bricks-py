import argparse
import logging
import os
import signal
import socket
import subprocess
import sys

from dbx_caddy import caddy

# CLI utility to run Reflex with Caddy in front so Databricks Apps style routing
# and HMR proxying work consistently in local and app-style environments.

LOG = logging.getLogger(__name__)
_DEFAULT_APP_PORT = 8000
_DEFAULT_BACKEND_PORT = 5000
_DEFAULT_FRONTEND_PORT = 5173


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="dbx-reflex-run",
        description="Run a Reflex app behind Caddy with HMR proxy support.",
    )
    parser.add_argument(
        "--backend-port",
        type=int,
        default=None,
        help="Backend port for Reflex API server.",
    )
    parser.add_argument(
        "--frontend-port",
        type=int,
        default=None,
        help="Frontend port for Reflex dev server (Vite).",
    )
    parser.add_argument(
        "--app-port",
        type=int,
        default=None,
        help="Public Caddy bind port (defaults to DATABRICKS_APP_PORT or 8000).",
    )
    parser.add_argument(
        "reflex_args",
        nargs=argparse.REMAINDER,
        help="Arguments forwarded to `reflex run`.",
    )
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
        f"    @websockets {{\n"
        f"        header Connection *Upgrade*\n"
        f"        header Upgrade websocket\n"
        f"    }}\n\n"
        f"    reverse_proxy @websockets localhost:{backend_port}\n"
        f"    reverse_proxy localhost:{frontend_port}\n"
        f"}}\n"
    )


def _default_reflex_args() -> list[str]:
    return ["--env", "dev", "--backend-only", "false"]


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


def _stop_reflex(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return

    LOG.info("Stopping Reflex process tree")
    try:
        if os.name == "posix":
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        else:
            proc.terminate()
    except Exception:
        LOG.exception("Failed to terminate Reflex process")

    try:
        proc.wait(timeout=10)
        return
    except subprocess.TimeoutExpired:
        LOG.warning("Reflex did not exit after SIGTERM, forcing kill")

    try:
        if os.name == "posix":
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        else:
            proc.kill()
    except Exception:
        LOG.exception("Failed to kill Reflex process")

    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        LOG.warning("Reflex process still running after SIGKILL")


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

    proc_holder: dict[str, subprocess.Popen] = {}

    def _handle_signal(signum, _frame):
        LOG.info("Received signal %s", signum)
        if proc := proc_holder.get("reflex"):
            _stop_reflex(proc)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    caddy_config = _caddyfile(
        app_port=app_port,
        backend_port=backend_port,
        frontend_port=frontend_port,
    )

    with caddy.run(caddy_config, "watch"):
        reflex_proc = _start_reflex(reflex_args=reflex_args, env=env)
        proc_holder["reflex"] = reflex_proc
        try:
            exit_code = reflex_proc.wait()
            return exit_code
        finally:
            _stop_reflex(reflex_proc)


if __name__ == "__main__":
    raise SystemExit(main())
