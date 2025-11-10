import fnmatch
import functools
import pathlib
import subprocess
from os import PathLike
from typing import Iterable

import benedict
import tomlkit

PYPROJECT_FILE_NAME = "pyproject.toml"


@functools.cache
def root() -> "Project":
    return Project(root_dir())


def dir(input: PathLike | str, match_member: bool = True) -> pathlib.Path | None:
    def _pyproject_file(path: pathlib.Path | None):
        if path is not None:
            if path.is_dir():
                return _pyproject_file(path / PYPROJECT_FILE_NAME)
            elif path.name != PYPROJECT_FILE_NAME:
                return _pyproject_file(path.parent)
            elif path.is_file():
                return path
        return None

    try:
        if f := _pyproject_file(pathlib.Path(input)):
            return f.parent
    except Exception:
        pass
    if match_member and isinstance(input, str):
        for p in root().members():
            if p.name == input:
                return p.dir
    return None


@functools.cache
def root_dir() -> pathlib.Path:
    """Return the repository root dir using git when available, else fall back to parent."""
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"], text=True
        ).strip()
        if d := dir(out, match_member=False) if out else None:
            return d
    except subprocess.CalledProcessError:
        pass
    # When git is unavailable or the file is outside a repo, use the parent dir
    if d := dir(__file__, match_member=False):
        return d
    raise ValueError("Root dir not found")


@functools.cache
def scripts_dir() -> pathlib.Path:
    file = pathlib.Path(__file__)
    for p in root().members():
        if file.is_relative_to(p.dir):
            return p.dir
    raise ValueError(f"Scripts dir not found: {file}")


class Project:

    def __init__(self, path: PathLike | str):
        self.dir = dir(path)
        if not self.dir:
            raise ValueError(f"Project dir not found: {path}")
        self.pyproject_file = self.dir / PYPROJECT_FILE_NAME
        try:
            pyproject_text = self.pyproject_file.read_text().rstrip()
            if pyproject_text:
                pyproject_text += "\n"
            pyproject_doc = tomlkit.parse(pyproject_text)
        except Exception as e:
            raise ValueError(f"Project {PYPROJECT_FILE_NAME} error - path:{self.pyproject_file} error:{e}")
        self.pyproject = benedict.benedict(pyproject_doc, keyattr_dynamic=True)

    @property
    def name(self):
        project_name = self.pyproject.get("project.name", None)
        return project_name or self.dir.name

    @property
    def is_root(self) -> bool:
        return root_dir() == self.dir

    @property
    def is_scripts(self) -> bool:
        return scripts_dir() == self.dir

    def members(self) -> Iterable["Project"]:
        for member_dir in self.member_dirs():
            yield Project(member_dir)

    def member_dirs(self) -> Iterable[pathlib.Path]:
        members = self.pyproject.get("tool.uv.workspace.members", [])
        exclude = self.pyproject.get("tool.uv.workspace.exclude", [])

        def match_any(name, patterns):
            return any(fnmatch.fnmatch(name, pat) for pat in patterns)

        for path in self.dir.iterdir():
            if not path.is_dir():
                continue
            name = path.name
            if match_any(name, members) and not match_any(name, exclude):
                if member_dir := dir(path):
                    yield member_dir

    def __str__(self):
        return f"{Project.__name__}(name={self.name!r} dir={self.dir.name!r})"


if __name__ == "__main__":
    print("-")
    print(root().name)
    print(root().pyproject)
    print(list(m.name for m in root().members()))
    print(scripts_dir())
    p = Project(root_dir() / "reggie-tools")
    p.pyproject.tool.test.example = "xyz12"
    print(p.name)
    print(tomlkit.dumps(p.pyproject))
