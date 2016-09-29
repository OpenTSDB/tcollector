#!/usr/bin/env python

import sys
import signal
import os
import time
from threading import Thread


class CollectorBase(object):
    def __init__(self, config, logger, readq):
        self._config = config
        self._logger = logger
        self._readq = readq
        self._exit = False
        """ long running collector need to check this flag to ensure responsive to shut down request to this collector"""

    def __call__(self, *arg):
        """
        any collector needs to implement it to collect metrics
        Returns: None

        """
        pass

    def cleanup(self):
        """
        any collector uses expensive OS resources like filehandles or sockets, etc. needs to close them here,
        Returns:None

        """
        pass

    def signal_exit(self):
        """
        signal collector to exit. any long running collector need to check _exit flag to ensure responsive to shut
        down request to this collector
        Returns:

        """
        self._exit = True

    # below are convenient methods available to all collectors
    def log_info(self, msg, *args, **kwargs):
        if self._logger:
            self._logger.info(msg, *args, **kwargs)
        else:
            sys.stdout.write("INFO: " + msg % args)

    def log_error(self, msg, *args, **kwargs):
        if self._logger:
            self._logger.error(msg, *args, **kwargs)
        else:
            sys.stderr.write("ERROR: " + msg % args)

    def log_warn(self, msg, *args, **kwargs):
        if self._logger:
            self._logger.warn(msg, *args, **kwargs)
        else:
            sys.stdout.write("WARN: " + msg % args)

    def log_exception(self, msg, *args, **kwargs):
        if self._logger:
            self._logger.exception(msg, *args, **kwargs)
        else:
            sys.stderr.write("ERROR: " + msg % args)

    def get_config(self, key, default=None, section='base'):
        if self._config and self._config.has_option(section, key):
            return self._config.get(section, key)
        else:
            return default

    def safe_close(self, handle):
        if handle:
            handle.close()

    def close_subprocess_async(self, proc, collector_name):
        if not proc:
            return

        if proc.poll() is None:
            tname = 'killsubproc-%s' % collector_name
            t = Thread(target=self.stop_subprocess, name=tname, args=(proc, collector_name))
            t.start()

    def stop_subprocess(self, proc, collector_name):
        pid = proc.pid
        try:
            _kill(pid)
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


def _kill(pid, signum=signal.SIGTERM):
        os.kill(pid, signum)
