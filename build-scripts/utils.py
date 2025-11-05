"""Small utility helpers used by build scripts."""

import pathlib
import subprocess


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
