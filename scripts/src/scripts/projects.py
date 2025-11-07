import fnmatch
import functools
import pathlib
import subprocess
from contextlib import contextmanager
from copy import deepcopy
from typing import Iterable

import tomli_w
from benedict import benedict
from benedict.dicts import IODict

_PYPROJECT_FILE_NAME = "pyproject.toml"


@functools.cache
def root_pyproject() -> "PyProject":
    """Return the repository root using git when available, else fall back to parent."""
    root_dir = None
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"], text=True
        ).strip()
        if out:
            root_dir = pathlib.Path(out)
    except subprocess.CalledProcessError:
        pass
    # When git is unavailable or the file is outside a repo, use the parent dir
    if root_dir is None:
        root_dir = pathlib.Path(__file__).resolve().parent
    pyproject = _to_pyproject_path(root_dir)
    if not pyproject:
        raise FileNotFoundError(f"Invalid {_PYPROJECT_FILE_NAME} path: {root_dir}")
    return PyProject(pyproject)


@functools.cache
def _scripts_pyproject() -> "PyProject":
    file = pathlib.Path(__file__)
    for p in root_pyproject().members(include_scripts=True):
        try:
            file.relative_to(p.pyproject.parent)
            return p
        except ValueError:
            pass
    raise FileNotFoundError(f"Script path not found: {file}")


def _to_pyproject_path(path) -> pathlib.Path:
    if path is not None:
        path = pathlib.Path(path)
        filename = _PYPROJECT_FILE_NAME
        if path.is_dir():
            path = path / filename
        elif path.name != filename:
            path = None
    return path


class PyProject:
    _data: IODict | None = None

    def __init__(self, path):
        self.pyproject = _to_pyproject_path(path)
        if not self.pyproject:
            raise FileNotFoundError(f"Invalid {_PYPROJECT_FILE_NAME} path: {path}")
        self.pyproject.parent.mkdir(parents=True, exist_ok=True)

    @property
    def data(self):
        if self._data is None:
            self._data = benedict.from_toml(str(self.pyproject),
                                            keyattr_dynamic=True) \
                if self.pyproject.is_file() else benedict({},
                                                          keyattr_dynamic=True)
        return self._data

    @contextmanager
    def edit(self):
        data_copy = deepcopy(self.data)
        try:
            yield data_copy
        except:
            raise

        else:
            if self.data != data_copy:
                print(f"writing pyproject data: {self.pyproject}")
                self._write_data(data_copy)
                self._data = data_copy

    @property
    def name(self):
        project_name = self.data.get("project.name", None)
        return project_name or self.pyproject.parent.name

    def members(self, include_scripts: bool = False) -> Iterable["PyProject"]:
        members = self.data.tool.uv.workspace.members or []
        exclude = self.data.tool.uv.workspace.exclude or []

        def match_any(name, patterns):
            return any(fnmatch.fnmatch(name, pat) for pat in patterns)

        for path in self.pyproject.parent.iterdir():
            if not path.is_dir():
                continue
            name = path.name
            if match_any(name, members) and not match_any(name, exclude):
                file = path / _PYPROJECT_FILE_NAME
                if file.is_file() and (include_scripts or file != _scripts_pyproject().pyproject):
                    yield PyProject(file)

    def write_data(self):
        d = self._data
        if d is None:
            return
        self._write_data(d)

    def _write_data(self, d):
        if d:
            self.pyproject.write_text(tomli_w.dumps(d, indent=4))
        elif self.pyproject.exists():
            self.pyproject.unlink()


if __name__ == "__main__":
    print(root_pyproject().name)
    print(_scripts_pyproject().name)
    p = root_pyproject()
    print(list(m.name for m in p.members()))
    print(p.name)
    print(p.data)
