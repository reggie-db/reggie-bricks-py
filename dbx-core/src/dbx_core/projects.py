"""Project name discovery helpers for modules, paths, and git repos."""

import functools
import pathlib
import subprocess
from os import PathLike


def root_dir(input: PathLike | str | None = None) -> pathlib.Path:
    if isinstance(input, str):
        return root_dir(pathlib.Path(input) if input else None)
    cwd = pathlib.Path.cwd().resolve()
    path = pathlib.Path(input).resolve() if input else None
    if path is None or path == cwd:
        return _root_dir_cwd(cwd)
    if result := _root_dir(path):
        return result
    raise ValueError(f"Could not determine root dir: {input}")


def _root_dir(path: pathlib.Path) -> pathlib.Path | None:
    if path.is_file():
        path = path.parent
    for proc_args in [
        ["uv", "workspace", "dir"],
        ["git", "rev-parse", "--show-toplevel"],
    ]:
        # noinspection PyBroadException
        try:
            proc = subprocess.run(
                proc_args,
                cwd=path,
                check=False,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
            if proc.returncode == 0:
                return pathlib.Path(proc.stdout.strip())
        except Exception:
            pass
    while path:
        if (path / "pyproject.toml").exists():
            return path
        path = path.parent
    raise None


@functools.lru_cache(maxsize=None)
def _root_dir_cwd(path: pathlib.Path) -> pathlib.Path:
    return _root_dir(path) or path


if __name__ == "__main__":
    print(root_dir(__file__))
    print(root_dir())
