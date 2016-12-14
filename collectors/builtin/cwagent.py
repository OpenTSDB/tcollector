#!/usr/bin/python
# This file is part of tcollector.
# Copyright (C) 2013  ProfitBricks GmbH

# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.  This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
# General Public License for more details.  You should have received a copy
# of the GNU Lesser General Public License along with this program.  If not,
# see <http://www.gnu.org/licenses/>.

"""
Collects statistics about running processes from /proc into TSDB.

Currently the following is collected:
 - Number of running tcollector processes
 - CPU and memory statistics from tcollector process and children

"""

import os
import resource
import time

from collectors.lib import utils
from collectors.lib.collectorbase import CollectorBase


class ProcessTerminatedError(Exception):
    pass


class Process(object):
    def __init__(self, pid):
        self.pid = pid

    def stat(self):
        """ Returns /proc/[pid]/stat as dict.

            The dict only contains the values that are currently used, but can
            be extended easily.
            If the process already terminated ProcessTerminatedError is raised.
        """

        path = "/proc/%s/stat" % self.pid
        try:
            with open(path) as f:
                spl = f.readline().split()
        except IOError:
            raise ProcessTerminatedError()

        rv = {"pid": spl[0], "comm": spl[1], "ppid": spl[3],
                "utime": spl[13], "stime": spl[14], "cutime": spl[15],
                "cstime": spl[16], "vsize": spl[22], "rss": spl[23]}
        # supported since Kernel 2.6.24
        if len(spl) > 43:
                rv.update({"guest_time": spl[42],
                           "cguest_time": spl[43]})
        return rv


class Cwagent(CollectorBase):
    def __init__(self, config, logger, readq):
        super(Cwagent, self).__init__(config, logger, readq)
        pid = os.getpid()
        self.log_info("cloudwiz agent pid %d", pid)
        self.process = Process(pid)

    def __call__(self):
        with utils.lower_privileges(self._logger):
            cpu_time = 0

            try:
                s = self.process.stat()
            except ProcessTerminatedError:
                self.log_warn("process terminated, abort")
                return

            cpu_time += int(s["utime"])
            cpu_time += int(s["cutime"])
            cpu_time += int(s["stime"])
            cpu_time += int(s["cstime"])
            cpu_time += int(s["guest_time"])
            cpu_time += int(s["cguest_time"])

            ts = int(time.time())
            self._readq.nput("cloudwiz-agent.cputime %s %s" % (ts, cpu_time))
            self._readq.nput("cloudwiz-agent.mem_bytes %s %s type=vsize" % (ts, s["vsize"]))
            self._readq.nput("cloudwiz-agent.mem_bytes %s %s type=rss" % (ts, int(s["rss"]) * resource.getpagesize()))


if __name__ == "__main__":
    cwagent_inst = Cwagent(None, None, utils.TestQueue())
    cwagent_inst()
