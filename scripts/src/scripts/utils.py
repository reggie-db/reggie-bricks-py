import logging
import sys

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


if __name__ == "__main__":
    logger(__name__).info("test")
