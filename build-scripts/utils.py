"""Small utility helpers used by build scripts."""

import pathlib
import subprocess
from typing import Iterable

# Common file name constants used across scripts
PY_PROJECT_FILE_NAME = "pyproject.toml"
# Alias used by member_project.py; keep both for compatibility
PYPROJECT_FILE_NAME = PY_PROJECT_FILE_NAME


def repo_root() -> pathlib.Path:
    """Return the repository root using git when available, else fall back to parent."""
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"], text=True
        ).strip()
        if out:
            return pathlib.Path(out)
    except Exception:
        pass
    # When git is unavailable or the file is outside a repo, use the parent dir
    return pathlib.Path(__file__).resolve().parents[1]


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


def workspace_projects(
    root: pathlib.Path, members: Iterable[str]
) -> list[pathlib.Path]:
    """Return a de-duplicated list of project directories for all workspace members."""
    projects: list[pathlib.Path] = []
    seen = set()
    for m in members:
        for proj in candidate_projects(root, m):
            rp = proj.resolve()
            if rp not in seen:
                seen.add(rp)
                projects.append(proj)
    return projects
