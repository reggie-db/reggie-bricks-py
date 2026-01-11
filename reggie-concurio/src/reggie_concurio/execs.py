import os
import uuid
from os import PathLike
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse
from urllib.request import urlretrieve

import lfp_logging.logs as logs
from reggie_core import objects, paths

from reggie_concurio import caches

LOG = logs.logger()

type InstallSource = Callable[[], PathLike] | PathLike | str

_CACHE = caches.DiskCache(__file__)


def executable(source: InstallSource, identifier: Any | None = None) -> "InstallPath":
    if identifier is None:
        identifier = source
    cache_key = objects.hash(identifier).hexdigest()
    return _CACHE.get_or_load(cache_key, loader=lambda: _install(source)).value


def _install(source: "InstallSource", eget: bool = True) -> "InstallPath":
    if isinstance(source, (Path, str)):
        path = paths.path(source, exists=True)
        if path is None:
            url = urlparse(source)

            def _download_source():
                temp_path = paths.temp_dir() / uuid.uuid4().hex
                urlretrieve(url.geturl(), temp_path)

                def _on_complete():
                    temp_path.unlink()

                return InstallPath(path=temp_path, on_complete=_on_complete)

            result_source = _download_source
        else:

            def _path_source():
                return path

            result_source = _path_source
    else:
        result_source = source
    result = objects.call(result_source)
    if not isinstance(result, InstallPath):
        result = InstallPath(path=result)
    if not os.access(result, os.X_OK):
        result.chmod(0o755)
    return result


def _eget():
    pass


class InstallPath(Path):
    def __init__(
        self,
        path: PathLike,
        on_complete: Callable[[PathLike], None] | Callable[[], None] | None = None,
    ):
        super().__init__(path)
        self._on_complete = on_complete

    def complete(self):
        if self._on_complete is not None:
            objects.call(self._on_complete, self)


if __name__ == "__main__":
    LOG.info("suh")
    source = "https://github.com/regclient/regclient/releases/download/v0.9.2/regctl-darwin-arm64"
    resource = executable(source)
    LOG.info(f"Installed resource: {resource}")
    # resource.complete()
