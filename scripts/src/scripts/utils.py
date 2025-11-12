import logging

from scripts import projects

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def logger(name: str | None = None):
    if not name:
        name = projects.root().name
    return logging.getLogger(name)
