import os
from pathlib import Path

import giturlparse
import requirements
import sh
from dbx_core import paths
from lfp_logging import logs
from requirements.requirement import Requirement


def is_url(source: str) -> bool:
    """Return True when the source is a valid git URL or git requirement string."""
    return _git_requirement(source) or giturlparse.validate(source)


def ssh_url(source: str) -> giturlparse.GitUrlParsed:
    """Return the SSH URL for a given source string when parseable."""
    git_url = _git_url(source)
    return git_url.url2ssh if git_url else None


def revision(source: str) -> str:
    """Resolve a preferred revision from a requirement string or URL, default main."""
    if requirement := _git_requirement(source):
        if requirement.revision:
            return requirement.revision
    if git_url := _git_url(source):
        if git_url.branch:
            return git_url.branch
    return "main"


def remote_commit_hash(source: str, token: str | None = None) -> str:
    """Fetch the remote commit hash for the given source and optional token."""
    remote_url = ssh_url(source)
    remote_revision = revision(source)
    output = _git_command(token)("ls-remote", remote_url, remote_revision)
    return output.strip().split()[0]


def clone(source: str, dest: str | Path, token: str | None = None) -> None:
    """Shallow clone source into dest at the resolved revision using optional token."""
    remote_url = ssh_url(source)
    remote_revision = revision(source)

    log = logs.logger()

    # Shallow single branch clone with progress logged to our logger
    _git_command(token)(
        "clone",
        "--branch",
        remote_revision,
        "--single-branch",
        "--depth",
        "1",
        remote_url,
        paths.path(dest, absolute=True),
        _out=lambda line: log.info(line.rstrip()),
        _err=lambda line: log.warning(line.rstrip()),
        _bg=True,
    ).wait()


def _git_command(token: str | None = None, **kwargs) -> sh.Command:
    """Return a baked ``git`` command with optional token injected into the env."""
    if token:
        env = os.environ.copy()
        env["GITHUB_TOKEN"] = token
        return sh.git.bake(_env=env, *kwargs)
    else:
        return sh.git


def _git_url(source: str) -> giturlparse.GitUrlParsed:
    """Parse a git URL from a requirement string or raw URL, else return None."""
    if requirement := _git_requirement(source):
        source = requirement.uri
    if giturlparse.validate(source):
        return giturlparse.parse(source)
    return None


def _git_requirement(source: str) -> Requirement:
    """Return a parsed requirements entry when the source is a git VCS spec."""
    try:
        for req in requirements.parse(source):
            if "git" == req.vcs and req.uri:
                return req
    except Exception:
        pass
    return None
