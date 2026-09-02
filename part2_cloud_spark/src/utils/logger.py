import logging
import sys

from config.config import LOG_LEVEL


logging.basicConfig(
    level=LOG_LEVEL,
    format="%(levelname)s | %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)