import os
from pathlib import Path

import giturlparse
import requirements
import sh
from reggie_core import logs, paths
from requirements.requirement import Requirement


def is_url(source: str) -> bool:
    return _git_requirement(source) or giturlparse.validate(source)


def ssh_url(source: str) -> giturlparse.GitUrlParsed:
    git_url = _git_url(source)
    return git_url.url2ssh if git_url else None


def revision(source: str) -> str:
    if requirement := _git_requirement(source):
        if requirement.revision:
            return requirement.revision
    if git_url := _git_url(source):
        if git_url.branch:
            return git_url.branch
    return "main"


def remote_commit_hash(source: str, token: str | None = None) -> str:
    remote_url = ssh_url(source)
    remote_revision = revision(source)
    output = _git_command(token)("ls-remote", remote_url, remote_revision)
    return output.strip().split()[0]


def clone(source: str, dest: str | Path, token: str | None = None) -> None:
    remote_url = ssh_url(source)
    remote_revision = revision(source)

    log = logs.logger("git_clone")

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
    if token:
        env = os.environ.copy()
        env["GITHUB_TOKEN"] = token
        return sh.git.bake(_env=env, *kwargs)
    else:
        return sh.git


def _git_url(source: str) -> giturlparse.GitUrlParsed:
    if requirement := _git_requirement(source):
        source = requirement.uri
    if giturlparse.validate(source):
        return giturlparse.parse(source)
    return None


def _git_requirement(source: str) -> Requirement:
    try:
        for req in requirements.parse(source):
            if "git" == req.vcs and req.uri:
                return req
    except Exception:
        pass
    return None
