"""Logging module"""

import logging
from logging.handlers import WatchedFileHandler
import sys

LOG = logging.getLogger(__name__)


def setup_logging(logger, logfile=None, debug=False):
    """Configure a logger"""

    fmt_str = "%(asctime)s: %(levelname)s:" \
              " %(module)s.%(funcName)s():%(lineno)d: %(message)s"

    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setFormatter(logging.Formatter(fmt_str))
    logger.handlers = []
    logger.addHandler(console_handler)
    if logfile:
        file_handler = WatchedFileHandler(logfile)
        file_handler.setFormatter(logging.Formatter(fmt_str))
        logger.addHandler(file_handler)
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
