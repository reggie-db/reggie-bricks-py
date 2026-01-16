"""
Project management utilities for working with Python projects and pyproject.toml files.

This module provides functionality to discover, load, and manipulate Python projects
within a workspace. It supports finding projects by name or path, reading and modifying
pyproject.toml configurations, and discovering workspace member projects.
"""

import fnmatch
import functools
import pathlib
import subprocess
from os import PathLike
from typing import Iterable

import tomlkit
from benedict.dicts import benedict
from reggie_core import logs

LOG = logs.logger(__file__)

# Standard filename for Python project configuration files
PYPROJECT_FILE_NAME = "pyproject.toml"


@functools.cache
def root() -> "Project":
    """
    Get the root workspace project.

    Returns a cached Project instance representing the root workspace directory.
    The root project is identified by the presence of a pyproject.toml file in the
    repository root directory.

    Returns:
        Project instance for the root workspace
    """
    return Project(root_dir())


def dir(input: PathLike | str, match_member: bool = True) -> pathlib.Path | None:
    """
    Find the project directory for a given input.

    Attempts to locate a project directory by searching for a pyproject.toml file.
    Can resolve paths, directory names, or project names. If match_member is True,
    will also search workspace member projects by name.

    Args:
        input: Path, directory name, or project name to search for
        match_member: If True, search workspace member projects by name when input is a string

    Returns:
        Path to the project directory if found, None otherwise
    """

    def _pyproject_file(path: pathlib.Path | None):
        """
        Recursively search for pyproject.toml file starting from the given path.

        Handles both directory and file paths, traversing up the directory tree
        if necessary to find the configuration file.

        Args:
            path: Starting path to search from

        Returns:
            Path to pyproject.toml file if found, None otherwise
        """
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
    # Search workspace member projects by name if input is a string
    if match_member and isinstance(input, str):
        for p in root().members():
            if p.name == input:
                return p.dir
    return None


@functools.cache
def root_dir() -> pathlib.Path:
    """
    Return the repository root directory.

    Attempts to find the workspace root using git when available. Falls back to
    locating the root by finding the pyproject.toml file relative to this module's
    location when git is unavailable or the workspace is outside a git repository.

    Returns:
        Path to the workspace root directory

    Raises:
        ValueError: If the root directory cannot be determined
    """
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
    """
    Return the directory containing the scripts project.

    Identifies which workspace member project contains this scripts module by
    checking if the current file is within any member project's directory.

    Returns:
        Path to the scripts project directory

    Raises:
        ValueError: If the scripts directory cannot be found in any member project
    """
    file = pathlib.Path(__file__)
    for p in root().members():
        if file.is_relative_to(p.dir):
            return p.dir
    raise ValueError(f"Scripts dir not found: {file}")


class Project:
    """
    Represents a Python project with its pyproject.toml configuration.

    Provides access to project metadata, configuration, and workspace relationships.
    The pyproject attribute is a mutable benedict that can be modified and persisted
    back to the pyproject.toml file.
    """

    def __init__(self, path: PathLike | str):
        """
        Initialize a Project instance from a path or project name.

        Locates the project directory, loads the pyproject.toml file, and parses
        it into a mutable benedict structure for easy manipulation.

        Args:
            path: Path to project directory, pyproject.toml file, or project name

        Raises:
            ValueError: If the project directory cannot be found or if pyproject.toml
                        cannot be parsed
        """
        self.dir = dir(path)
        if not self.dir:
            raise ValueError(f"Project dir not found: {path}")
        self.pyproject_file = self.dir / PYPROJECT_FILE_NAME
        try:
            pyproject_text = self.pyproject_file.read_text().rstrip()
            # Ensure trailing newline for proper TOML parsing
            if pyproject_text:
                pyproject_text += "\n"
            pyproject_doc = tomlkit.parse(pyproject_text)
        except Exception as e:
            raise ValueError(
                f"Project {PYPROJECT_FILE_NAME} error - path:{self.pyproject_file} error:{e}"
            )
        # Use benedict for dynamic attribute access and easy merging
        self.pyproject = benedict(pyproject_doc, keyattr_dynamic=True)

    @property
    def name(self):
        """
        Get the project name.

        Returns the name from pyproject.toml's project.name field, or falls back
        to the directory name if not specified.

        Returns:
            Project name string
        """
        project_name = self.pyproject.get("project.name", None)
        return project_name or self.dir.name

    @property
    def is_root(self) -> bool:
        """
        Check if this project is the root workspace project.

        Returns:
            True if this project is the root workspace, False otherwise
        """
        return root_dir() == self.dir

    @property
    def is_scripts(self) -> bool:
        """
        Check if this project is the scripts project.

        Returns:
            True if this project is the scripts project, False otherwise
        """
        return scripts_dir() == self.dir

    def members(self) -> Iterable["Project"]:
        """
        Get all workspace member projects.

        Yields Project instances for each member project defined in this project's
        tool.uv.workspace.members configuration. Only includes members that match
        the include patterns and don't match exclude patterns.

        Yields:
            Project instances for each member project
        """
        for member_dir in self.member_dirs():
            yield Project(member_dir)

    def member_dirs(self) -> Iterable[pathlib.Path]:
        """
        Get directory paths for all workspace member projects.

        Scans the project directory for subdirectories that match the workspace
        member patterns and don't match exclude patterns. Only returns directories
        that contain valid pyproject.toml files.

        Yields:
            Path objects for each valid member project directory
        """
        members = self.pyproject.get("tool.uv.workspace.members", [])
        exclude = self.pyproject.get("tool.uv.workspace.exclude", [])

        def match_any(name, patterns):
            """
            Check if a name matches any pattern using fnmatch glob matching.

            Args:
                name: Name to check
                patterns: List of glob patterns to match against

            Returns:
                True if name matches any pattern, False otherwise
            """
            return any(fnmatch.fnmatch(name, pat) for pat in patterns)

        for path in self.dir.iterdir():
            if not path.is_dir():
                continue
            name = path.name
            # Include if matches member patterns and doesn't match exclude patterns
            if match_any(name, members) and not match_any(name, exclude):
                if member_dir := dir(path):
                    yield member_dir

    def __str__(self):
        """
        Return a string representation of the project.

        Returns:
            String showing the project name and directory name
        """
        return f"{Project.__name__}(name={self.name!r} dir={self.dir.name!r})"


if __name__ == "__main__":
    LOG.info("-")
    LOG.info(root().name)
    LOG.info(root().pyproject)
    LOG.info(list(m.name for m in root().members()))
    LOG.info(scripts_dir())
    p = Project(root_dir() / "dbx-tools")
    p.pyproject.tool.test.example = "xyz12"
    LOG.info(p.name)
    LOG.info(tomlkit.dumps(p.pyproject))
