import argparse
import logging
import os
import signal
import socket
import subprocess
import sys
import time

from dbx_caddy import caddy

# CLI utility to run Reflex with Caddy in front so Databricks Apps style routing
# and HMR proxying work consistently in local and app-style environments.

LOG = logging.getLogger(__name__)
_DEFAULT_APP_PORT = 8000


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
    # Match dbx-lottery behavior: backend/frontend ports are randomized each run
    # unless explicitly provided by CLI flags.
    backend_port = args.backend_port or _get_free_port()
    frontend_port = args.frontend_port or _get_free_port()
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


def _strip_mode_flags(reflex_args: list[str]) -> list[str]:
    """Remove frontend/backend mode flags from forwarded args.

    This runner controls frontend/backend mode explicitly for each child process.
    """
    out: list[str] = []
    skip_next = False
    for idx, arg in enumerate(reflex_args):
        if skip_next:
            skip_next = False
            continue
        if arg in (
            "--backend-only",
            "--frontend-only",
            "--backend_port",
            "--frontend_port",
            "--backend-port",
            "--frontend-port",
        ):
            if idx + 1 < len(reflex_args) and not reflex_args[idx + 1].startswith("-"):
                skip_next = True
            continue
        out.append(arg)
    return out


def _start_reflex(
    reflex_args: list[str],
    env: dict[str, str],
    *,
    backend_only: bool,
    frontend_only: bool,
) -> subprocess.Popen:
    cmd = ["reflex", "run", *reflex_args]
    LOG.info("Starting Reflex: %s", " ".join(cmd))
    env = dict(env)
    env["REFLEX_BACKEND_ONLY"] = "true" if backend_only else "false"
    env["REFLEX_FRONTEND_ONLY"] = "true" if frontend_only else "false"
    if os.name == "posix":
        return subprocess.Popen(cmd, env=env, preexec_fn=os.setsid)
    return subprocess.Popen(cmd, env=env)


def _kill_process(proc: subprocess.Popen) -> None:
    """Last-resort kill that always runs."""
    try:
        if os.name == "posix":
            os.killpg(proc.pid, signal.SIGKILL)
        else:
            proc.kill()
    except ProcessLookupError:
        pass
    except Exception as e:
        LOG.debug("Force kill failed pid=%s error=%s", proc.pid, e)

    try:
        proc.wait(timeout=5)
    except Exception:
        pass


def _stop_process(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return

    LOG.info("Stopping reflex process - pid: %s", proc.pid)

    try:
        # ---------------- graceful shutdown ----------------
        try:
            if os.name == "posix":
                os.killpg(proc.pid, signal.SIGTERM)
            else:
                proc.terminate()
        except ProcessLookupError:
            return
        except Exception as e:
            LOG.debug("SIGTERM failed pid=%s error=%s", proc.pid, e)
            _kill_process(proc)
            return

        try:
            proc.wait(timeout=10)
            return
        except subprocess.TimeoutExpired:
            LOG.info("Reflex still running after SIGTERM - escalating")

        # ---------------- hard shutdown ----------------
        try:
            if os.name == "posix":
                os.killpg(proc.pid, signal.SIGKILL)
            else:
                proc.kill()
        except ProcessLookupError:
            return
        except Exception as e:
            LOG.debug("SIGKILL failed pid=%s error=%s", proc.pid, e)
            _kill_process(proc)
            return

        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            LOG.warning("Process would not exit pid=%s", proc.pid)
            _kill_process(proc)

    # absolutely anything unexpected
    except BaseException as e:
        LOG.exception("Unexpected error stopping reflex pid=%s", proc.pid)
        _kill_process(proc)


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
    reflex_args = _strip_mode_flags(_normalize_reflex_args(args.reflex_args))

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

    caddy_config = _caddyfile(
        app_port=app_port,
        backend_port=backend_port,
        frontend_port=frontend_port,
    )

    with caddy.run(caddy_config, "watch") as caddy_proc:
        reflex_backend = _start_reflex(
            reflex_args=reflex_args,
            env=env,
            backend_only=True,
            frontend_only=False,
        )
        reflex_frontend = _start_reflex(
            reflex_args=reflex_args,
            env=env,
            backend_only=False,
            frontend_only=True,
        )
        reflex_processes = [reflex_backend, reflex_frontend]
        proc_holder["reflex"] = reflex_processes
        try:
            idx, exit_code = _wait_until_any_exits([*reflex_processes, caddy_proc])
            if idx == 0:
                LOG.error("Reflex backend exited with code %s", exit_code)
            elif idx == 1:
                LOG.error("Reflex frontend exited with code %s", exit_code)
            else:
                LOG.error("Caddy exited with code %s", exit_code)
            return exit_code
        finally:
            _stop_all(reflex_processes)


if __name__ == "__main__":
    raise SystemExit(main())
