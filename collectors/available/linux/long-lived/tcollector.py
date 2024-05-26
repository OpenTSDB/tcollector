#!/usr/bin/env python
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
import pwd
import resource
import sys
import time

from collectors.lib import utils

COLLECTION_INTERVAL = 15  # seconds


class ProcessTerminatedError(Exception):
    pass


class Process(object):
    def __init__(self, pid):
        self.pid = pid
        stat = self.stat()
        self.comm = stat["comm"].strip("()")
        self.ppid = int(stat["ppid"])
        self._cmdline = None

    @property
    def cmdline(self):
        """ Returns /proc/[pid]/cmdline as tuple.
            If the process already terminated ProcessTerminatedError is raised.
        """

        if self._cmdline is None:
            path = "/proc/%s/cmdline" % self.pid
            try:
                with open(path) as f:
                    cmdl = f.readline()
                    if cmdl:
                        self._cmdline = tuple(cmdl.split('\0'))
            except IOError:
                raise ProcessTerminatedError()

        return self._cmdline

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


class ProcessTable(object):
    """ List of all running processes.

    Process informations are gathered from /proc.
    """
    def __init__(self):
        self.processes = {}
        self.update()

    def update(self):
        new = {}
        pids = [int(i) for i in os.listdir("/proc") if i.isdigit()]
        for pid in pids:
            # TODO: Optimize: Don't create 2 objects, use a factory function
            # or something similar
            if pid in self.processes:
                new[pid] = self.processes[pid]
            else:
                try:
                    p = Process(pid)
                    new[pid] = p
                except ProcessTerminatedError:
                    continue
        self.processes = new

    def filter(self, cond):
        """ Return processes for that the function cond evaluates to true. """
        return list(filter(cond, self.processes.values()))

def collect_tcollect_stats(processes):
    # print a msg and do nothing if the parent process isn't tcollector
    # (eg when processtats.py is executed from shell)
    tcol_pid = os.getppid()
    tcol_process = Process(tcol_pid)
    if not "tcollector.py" in " ".join(tcol_process.cmdline):
        sys.stderr.write("Parent Process %s isn't a tcollector instance\n" %
                tcol_pid)
        return

    tcollect_procs = processes.filter(lambda p: p.ppid == tcol_pid)
    ts = int(time.time())
    print("tcollector.processes %s %s" % (ts, len(tcollect_procs)))

    for p in tcollect_procs:
        cpu_time = 0

        try:
            s = p.stat()
        except ProcessTerminatedError:
            continue

        cpu_time += int(s["utime"])
        cpu_time += int(s["cutime"])
        cpu_time += int(s["stime"])
        cpu_time += int(s["cstime"])
        cpu_time += int(s["guest_time"])
        cpu_time += int(s["cguest_time"])

        # if <collector> is executed with "python <collector.py>",
        # ensure <collector.py> is used as name
        if p.comm == "python" and p.cmdline is not None:
            comm = p.cmdline[1].split('/')[-1]
        else:
            comm = p.comm

        print("tcollector.cputime %s %s name=%s" % (ts, cpu_time, comm))
        print("tcollector.mem_bytes %s %s name=%s type=vsize" %
                (ts, s["vsize"], comm))
        print("tcollector.mem_bytes %s %s name=%s type=rss" %
                (ts, int(s["rss"]) * resource.getpagesize(), comm))


def main():
    utils.drop_privileges()

    while True:
        processes = ProcessTable()
        processes.update()
        collect_tcollect_stats(processes)

        time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
    main()
