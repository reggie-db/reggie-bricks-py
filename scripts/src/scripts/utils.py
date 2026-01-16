import hashlib
import logging
import pathlib
import sys
import time

from scripts import projects

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)


def logger(name: str | None = None):
    if not name:
        name = projects.root().name
    return logging.getLogger(name)


def watch_file(src: pathlib.Path, interval: float = 2.0):
    """Yield the current file hash each time it changes"""

    def _hash(p: pathlib.Path) -> str:
        h = hashlib.sha256()
        with open(p, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()

    last_hash = None
    while True:
        try:
            current = _hash(src)
            if current != last_hash:
                last_hash = current
                yield current
            time.sleep(interval)
        except FileNotFoundError:
            time.sleep(interval)
        except KeyboardInterrupt:
            return


if __name__ == "__main__":
    logger(__name__).info("test")
