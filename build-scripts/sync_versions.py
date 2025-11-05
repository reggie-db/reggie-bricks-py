import logging
import pathlib
import subprocess

import tomli_w
import tomllib
import utils

logging.basicConfig(level=logging.INFO, format="%(message)s")
LOG = logging.getLogger(__name__)

DEFAULT_VERSION = "0.0.1"
PY_PROJECT_FILE_NAME = "pyproject.toml"
VERSION_KEY = "version"
DYNAMIC_KEY = "dynamic"

"""Synchronize project versions across workspace members based on git revision."""


def candidate_projects(root: pathlib.Path, member_pattern: str) -> list[pathlib.Path]:
    """Return project directories matching the member glob, including subdirs."""
    paths: list[pathlib.Path] = []
    for p in root.glob(member_pattern):
        if not p.is_dir():
            continue
        pj = p / PY_PROJECT_FILE_NAME
        if pj.exists():
            paths.append(p)
        else:
            for child in p.iterdir():
                if child.is_dir() and (child / PY_PROJECT_FILE_NAME).exists():
                    paths.append(child)
    return paths


def version() -> str:
    """Build a workspace version string of the form 0.0.1+g<rev> when git is available."""
    try:
        rev = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=pathlib.Path(__file__).resolve().parents[1],
            text=True,
        ).strip()
        if rev:
            return f"{DEFAULT_VERSION}+g{rev}"
    except Exception:
        pass
    return DEFAULT_VERSION


def main():
    """Entry point: compute version and write it into each member's pyproject."""
    root = utils.repo_root()
    pyproject_version = version()
    pyproject_root = tomllib.loads((root / PY_PROJECT_FILE_NAME).read_text())
    members = (
        pyproject_root.get("tool", {})
        .get("uv", {})
        .get("workspace", {})
        .get("members", [])
    )
    if not members:
        raise SystemExit("No workspace members found under [tool.uv.workspace].")

    projects: list[pathlib.Path] = []
    seen = set()
    for m in members:
        for proj in candidate_projects(root, m):
            rp = proj.resolve()
            if rp not in seen:
                seen.add(rp)
                projects.append(proj)

    for proj in projects:
        py_path = proj / PY_PROJECT_FILE_NAME
        data = tomllib.loads(py_path.read_text())

        proj_tbl = data.setdefault("project", {})
        current = proj_tbl.get(VERSION_KEY, "<none>")

        # If version was dynamic, remove it so a static version applies
        dyn = proj_tbl.get(DYNAMIC_KEY)
        if isinstance(dyn, list) and VERSION_KEY in dyn:
            proj_tbl[DYNAMIC_KEY] = [x for x in dyn if x != VERSION_KEY]
            if not proj_tbl[DYNAMIC_KEY]:
                proj_tbl.pop(DYNAMIC_KEY)

        proj_tbl[VERSION_KEY] = pyproject_version
        LOG.info(f"{proj.name}: {current} -> {pyproject_version}")

        py_path.write_text(tomli_w.dumps(data))


if __name__ == "__main__":
    main()
