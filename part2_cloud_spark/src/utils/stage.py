import time
from contextlib import contextmanager

from utils.logger import logger


@contextmanager
def stage(name):
    logger.info("STAGE START | %s", name)
    start = time.monotonic()
    try:
        yield
    except Exception as e:
        logger.error("STAGE FAILED | %s | duration_s=%.1f | error=%s",
                     name, time.monotonic() - start, e)
        raise
    else:
        logger.info("STAGE OK | %s | duration_s=%.1f",
                     name, time.monotonic() - start)