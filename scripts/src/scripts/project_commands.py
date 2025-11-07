import argparse
import pathlib
import subprocess
import sys

from scripts.projects import root_pyproject, PyProject

_DEFAULT_VERSION = "0.0.1"


def update_versions(version: str | None = None):
    if not version:
        version = _version()
    pyproject = root_pyproject()
    for p in [pyproject] + list(pyproject.members()):
        _set_version(p, version)


def _set_version(pyproject: PyProject, version: str):
    with pyproject.edit() as data:
        project = data.get("project", None)
        if project:
            if version:
                project.version = version
            elif "version" in data:
                del project["version"]


def _version() -> str:
    """Build a workspace version string of the form 0.0.1+g<rev> when git is available."""
    try:
        rev = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=pathlib.Path(__file__).resolve().parents[1],
            text=True,
        ).strip()
        if rev:
            return f"{_DEFAULT_VERSION}+g{rev}"
    except Exception:
        pass
    return _DEFAULT_VERSION


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", help="Method name to run")
    parser.add_argument("args", nargs=argparse.REMAINDER)
    parsed = parser.parse_args()

    cmd = parsed.command
    func = globals().get(cmd) if cmd and not cmd.startswith("_") else None
    if not callable(func):
        print(f"Unknown command: {cmd}", file=sys.stderr)
        sys.exit(1)

    func(*parsed.args)


if __name__ == "__main__":
    main()
