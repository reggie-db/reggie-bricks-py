import os
import shutil
from pathlib import Path

import utils

root = utils.repo_root()
root_venv = root / ".venv"
build_scripts_root = utils.build_scripts_root()

EXCLUDES = [
    lambda path: path.name == ".venv" and path.parent == root,
    lambda path: utils.build_scripts_root() in path.parents,
]
MATCHERS = [
    lambda path: path.name == ".venv",
    lambda path: path.name == "__pycache__" and path.parent != root_venv,
    lambda path: path.name.endswith(".egg-info"),
]


def run():
    for dirpath, dirnames, _ in os.walk(root):
        path = Path(dirpath)
        if any(f(path) for f in EXCLUDES):
            dirnames[:] = []
            continue
        if any(f(path) for f in MATCHERS):
            dirnames[:] = []
            print(f"Deleting directory:{path}")
            shutil.rmtree(path)


if __name__ == "__main__":
    run()
