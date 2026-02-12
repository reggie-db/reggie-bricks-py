#!/usr/bin/env python3
from __future__ import annotations

import getpass
import os
import pathlib
import shutil
import subprocess
import time
from functools import cache
from pathlib import Path
from typing import Any

from dbx_core import projects
from sqlalchemy import URL, text

from dbx_postgres import postgres

# ---------- utils ----------


@cache
def _pixi() -> bool:
    pproject, pdata = projects.root_pyproject()
    if pdata.get("tool", {}).get("pixi", None) is not None:
        return True
    return False


@cache
def _bin_dir():
    if cmd_path := shutil.which("postgres"):
        return Path(cmd_path).parent
    pproject, pdata = projects.root_pyproject()
    if pdata.get("tool", {}).get("pixi", None) is not None:
        pixi_run = ["pixi", "run", "which", "postgres"]
        proc = subprocess.run(pixi_run, stdout=subprocess.PIPE)
        if proc.returncode == 0:
            return Path(proc.stdout.decode().strip()).parent
    raise FileNotFoundError(f"Postgres binary not found")


def _run(cmds: list[Any], quiet=True) -> subprocess.CompletedProcess[bytes]:
    if cmds:
        cmds = [_bin_dir() / cmds[0]] + cmds[1:]
    return subprocess.run(
        [str(c) for c in cmds],
        check=True,
        stdout=subprocess.DEVNULL if quiet else None,
        stderr=subprocess.DEVNULL if quiet else None,
    )


class LocalPostgres:
    def __init__(self, project: str | None = None):
        if not project:
            project = projects.root_project_name()
        if not project:
            raise ValueError("project required")

        self.project = project

        home = Path.home()
        self.base_dir = home / f".{project}" / "postgres"
        self.socket_dir = self.base_dir / "_socket"
        self.data_dir = self.base_dir / "data"
        self.pid_file = self.data_dir / "postmaster.pid"
        self.log_file = self.base_dir / "postgres.log"

        self.base_dir.mkdir(parents=True, exist_ok=True)

    # ---------- process state ----------

    def _is_running(self) -> bool:
        if not self.pid_file.exists():
            return False

        try:
            pid = self.pid_file.read_text().splitlines()[0].strip()
            if not pid:
                return False
            os.kill(int(pid), 0)
            return True
        except Exception:
            return False

    # ---------- readiness ----------

    def _wait_ready(self):
        for _ in range(80):
            if list(self.socket_dir.glob(".s.PGSQL.*")):
                r = _run(["pg_isready", "-h", str(self.socket_dir)])
                if r.returncode == 0:
                    return
            time.sleep(0.25)
        raise RuntimeError("postgres did not become ready")

    # ---------- initialization ----------

    def _init_db(self):
        self.data_dir.mkdir(parents=True, exist_ok=True)

        _run(
            [
                "initdb",
                "-D",
                self.data_dir,
                "--auth=trust",
                "--encoding=UTF8",
                "--locale=C",
                "--lc-collate=C",
                "--lc-ctype=C",
            ]
        )

        with open(self.data_dir / "postgresql.conf", "a") as f:
            f.write("listen_addresses = ''\n")
            f.write(f"unix_socket_directories = '{self.socket_dir}'\n")

        with open(self.data_dir / "pg_hba.conf", "a") as f:
            f.write(
                "local   all             all                                     trust\n"
                "host    all             all             127.0.0.1/32            reject\n"
                "host    all             all             ::1/128                 reject\n"
            )

    # ---------- public API ----------

    def up(self) -> pathlib.Path:
        self.socket_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(self.socket_dir, 0o700)

        if not self._is_running():
            if not self.data_dir.exists():
                self._init_db()
            _run(
                [
                    "pg_ctl",
                    "-D",
                    self.data_dir,
                    "-l",
                    self.log_file,
                    "start",
                ]
            )
            self._wait_ready()
        return self.socket_dir.resolve()

    def down(self):
        if not self.data_dir.exists():
            return

        if self._is_running():
            _run(["pg_ctl", "-D", str(self.data_dir), "-m", "fast", "stop"])

    def url(self, db_name: str, up: bool = True) -> URL:
        if up:
            self.up()
        return URL.create(
            drivername="postgresql+psycopg",
            username=getpass.getuser(),
            database=db_name,
            query={"host": str(self.socket_dir.resolve())},
        )


# ---------- convenience CLI ----------

if __name__ == "__main__":
    os.chdir("/Users/reggie.pierce/Projects/reggie-ai")
    import sys

    if len(sys.argv) > 1:
        action = sys.argv[1]
    else:
        action = "up"
    if len(sys.argv) > 2:
        project = sys.argv[2]
    else:
        project = None

    svc = LocalPostgres(project)

    if action == "up":
        engine = postgres.create_engine(svc.url("apple_mail"))
        with engine.connect() as conn:
            result = conn.execute(text("select 1"))
            print(result.scalar())
        engine.dispose()

    elif action == "down":
        svc.down()
    else:
        print("invalid action", file=sys.stderr)
        sys.exit(1)
