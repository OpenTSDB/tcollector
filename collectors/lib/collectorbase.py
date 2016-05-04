#!/usr/bin/env python

import sys
import signal
import os
import time
from threading import Thread
from Queue import Queue


class CollectorBase(object):
    def __init__(self, config, logger, readq):
        self._config = config
        self._logger = logger
        self._readq = readq

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

    def log_exception(self, msg, *args, **kwargs):
        if self._logger:
            self._logger.exception(msg, args, kwargs)
        else:
            sys.stderr.write("ERROR: " + msg % args)

    def get_config(self, key, default=None, section='base'):
        if self._config and self._config.has_option(section, key):
            return self._config.get(section, key)
        else:
            return default

    def safe_close(self, filehandle):
        if filehandle:
            filehandle.close()

    def close(self):
        """ any collector uses expensive OS resources like filehandles or sockets, etc. needs to close them here
        Returns:None

        """
        pass

    def close_subprocess_async(self, proc, collector_name):
        if not proc:
            return

        if proc.poll() is None:
            tname = 'killsubproc-%s' % collector_name
            t = Thread(target=self._stop_subprocess, name=tname, args=(proc, collector_name))
            t.start()

    def _stop_subprocess(self, proc, collector_name):
        pid = proc.pid
        try:
            _kill(proc)
            for attempt in range(5):
                if proc.poll() is not None:
                    return
                self.log_info('Waiting %ds for PID %d (%s) to exit...', 5 - attempt, pid, collector_name)
                time.sleep(1)

            self.log_info('force kill PID %d', pid)
            _kill(proc, signal.SIGKILL)
            proc.wait()
            self.log_info('%s killed', pid)
        except:
            self.log_exception('ignoring uncaught exception while close subprocess %d', proc.pid)


def _kill(proc, signum=signal.SIGTERM):
        os.killpg(proc.pid, signum)
