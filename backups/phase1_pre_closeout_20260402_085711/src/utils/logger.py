from __future__ import annotations

import sys

from loguru import logger


def configure_logging(level: str = "INFO"):
    logger.remove()
    try:
        logger.add(sys.stderr, level=level, enqueue=True)
    except PermissionError:
        # Some restricted Windows environments disallow multiprocessing pipes.
        logger.add(sys.stderr, level=level, enqueue=False)
    return logger
