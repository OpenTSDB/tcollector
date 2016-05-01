#!/usr/bin/env python

import sys


class CollectorBase(object):
    def __init__(self, config, logger):
        self._config = config
        self._logger = logger

    def log_info(self, msg, *args):
        if self._logger:
            self._logger.info(msg, args)
        else:
            sys.stdout.write("INFO: " + msg % args)

    def log_error(self, msg, *args):
        if self._logger:
            self._logger.error(msg, args)
        else:
            sys.stderr.write("ERROR: " + msg % args)

    def log_warn(self, msg, *args):
        if self._logger:
            self._logger.warn(msg, args)
        else:
            sys.stdout.write("WARN: " + msg % args)

    def get_config(self, key, default, section='base'):
        if self._config and self._config.has_option(section, key):
            return self._config.get(section, key)
        else:
            return default
