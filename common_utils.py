#!/usr/bin/env python

import logging
import sys
from logging.handlers import RotatingFileHandler


# noinspection SpellCheckingInspection
def setup_logging(logger, logfile, max_bytes=None, backup_count=None):
    """Sets up logging and associated handlers."""

    logger.setLevel(logging.INFO)
    if backup_count is not None and max_bytes is not None:
        assert backup_count > 0
        assert max_bytes > 0
        ch = RotatingFileHandler(logfile, 'a', max_bytes, backup_count)
    else:  # Setup stream handler.
        ch = logging.StreamHandler(sys.stdout)

    ch.setFormatter(logging.Formatter('%(asctime)s %(module)s[%(process)d:%(thread)d]:%(lineno)d '
                                      '%(levelname)s: %(message)s'))
    logger.addHandler(ch)
