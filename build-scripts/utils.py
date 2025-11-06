"""Small utility helpers used by build scripts."""

import pathlib
import subprocess
import sys
from contextlib import contextmanager
from copy import deepcopy
from typing import Iterator

import tomli_w
from packaging.specifiers import SpecifierSet
from packaging.version import Version

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


# Common file name constants used across scripts
PYPROJECT_FILE_NAME = "pyproject.toml"


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


def build_scripts_root() -> pathlib.Path:
    file_path = pathlib.Path(__file__).resolve()
    for child in repo_root().iterdir():
        if not child.is_dir():
            continue
        # recursive search inside this child
        try:
            if file_path.is_relative_to(child.resolve()):
                return child
        except AttributeError:
            # Python before is_relative_to
            try:
                file_path.relative_to(child.resolve())
                return child
            except ValueError:
                pass
    raise FileNotFoundError(f"Could not find build-scripts root in {repo_root()}")


def workspace_pyprojects(root: pathlib.Path | None = None) -> Iterator["PyProject"]:
    if not root:
        root = repo_root()
    for project_path in _workspace_pyproject_paths(root):
        yield PyProject(project_path)


def _workspace_pyproject_paths(root: pathlib.Path) -> Iterator[pathlib.Path]:
    """Return a de-duplicated iterator of project directories for all workspace members."""

    pyproject = PyProject(root)
    seen = set()
    for m in pyproject.members:
        for project_path in _pyproject_dirs(root, m):
            project_path = project_path.resolve()
            if project_path not in seen:
                seen.add(project_path)
                yield project_path


def _pyproject_dirs(root: pathlib.Path, member: str) -> Iterator[pathlib.Path]:
    """Return project directories matching the member glob, including subdirs."""
    for p in root.glob(member):
        if not p.is_dir():
            continue
        pj = p / PYPROJECT_FILE_NAME
        if pj.exists():
            yield p
        else:
            for child in p.iterdir():
                if child.is_dir() and (child / PYPROJECT_FILE_NAME).exists():
                    yield child


class PyProject:
    def __init__(self, project_path: pathlib.Path):
        if project_path.is_file():
            project_path = project_path.parent
        self.pyproject_path = project_path / PYPROJECT_FILE_NAME

    @contextmanager
    def pyproject(self):
        path = self.pyproject_path
        if path.exists():
            data = tomllib.loads(path.read_text())
        else:
            data = {}
        original_data = deepcopy(data)
        try:
            yield data
        except:
            raise
        else:
            if original_data != data:
                print(f"writing pyproject data: {path}")
                if not data:
                    if path.exists():
                        path.unlink()
                else:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(tomli_w.dumps(data))

    @contextmanager
    def project(self):
        with self.pyproject() as pyproject:
            key = "project"
            project = pyproject.setdefault(key, {})
            try:
                yield project
            except Exception:
                raise
            else:
                if project:
                    if "name" not in project:
                        project["name"] = self.pyproject_path.parent.name.replace(
                            "-", "_"
                        )
                    pyproject[key] = project
                else:
                    del pyproject[key]

    @property
    def members(self) -> list[str]:
        with self.pyproject() as pyproject:
            members = (
                pyproject.get("tool", {})
                .get("uv", {})
                .get("workspace", {})
                .get("members", [])
            )
            return members

    @contextmanager
    def requires_python(self):
        with self.project() as project:
            key = "requires-python"
            specifier_set_original = PyProject._specifier_set(project.get(key, None))
            requires_python = (
                [f"{v.operator}{v.version}" for v in specifier_set_original]
                if specifier_set_original
                else []
            )
            try:
                yield requires_python
            except Exception:
                raise
            else:
                if not requires_python:
                    requires_python.append(f"=={sys.version.split()[0]}")
                specifier_set = PyProject._specifier_set(requires_python)
                if specifier_set != specifier_set_original:
                    project[key] = PyProject._specifier_set_to_str(specifier_set)

    @property
    def requires_python_min(self) -> Version | None:
        return self._requires_python_min(">", ">=", "==")

    @property
    def requires_python_max(self) -> Version | None:
        return self._requires_python_min("<", "<=")

    def _requires_python_min(self, *operators: str) -> Version | None:
        if operators:
            operators = list(operators)
            with self.requires_python() as requires_python:
                specifier_set = PyProject._specifier_set(requires_python)
                if specifier_set:
                    result: Version | None = None
                    for specifier in specifier_set:
                        if specifier.operator in operators:
                            specifier_version = Version(specifier.version)
                            if result is None or result > specifier_version:
                                result = specifier_version
                    return result
        return None

    @staticmethod
    def _specifier_set(
        specifier: SpecifierSet | list[str] | str | None,
    ) -> SpecifierSet | None:
        if not specifier:
            return None
        if isinstance(specifier, list):
            specifier = SpecifierSet(",".join(specifier))
        if isinstance(specifier, str):
            specifier = SpecifierSet(specifier)
        return specifier

    @staticmethod
    def _specifier_set_to_str(
        specifier: SpecifierSet | list[str] | str | None,
    ) -> str | None:
        specifier = PyProject._specifier_set(specifier)
        result = ",".join([f"{v.operator}{v.version}" for v in (specifier or [])])
        return result


if __name__ == "__main__":
    project = PyProject((build_scripts_root() / PYPROJECT_FILE_NAME).absolute())
    print(project.requires_python_min)
    print(project.requires_python_max)
    with project.requires_python() as requires_python:
        print(requires_python)
        requires_python.clear()
        requires_python.append(">=3.10")
        print(requires_python)
    print(project.requires_python_min)
    print(project.requires_python_max)
