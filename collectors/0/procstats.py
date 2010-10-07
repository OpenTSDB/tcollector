#!/usr/bin/python
# This file is part of tcollector.
# Copyright (C) 2010  StumbleUpon, Inc.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.  This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
# General Public License for more details.  You should have received a copy
# of the GNU Lesser General Public License along with this program.  If not,
# see <http://www.gnu.org/licenses/>.
#
# import various stats from /proc into TSDB

import os
import sys
import time
import subprocess
import socket
import re


def main():
    interval = 15

    f_meminfo = open("/proc/meminfo", "r")
    f_vmstat = open("/proc/vmstat", "r")
    f_stat = open("/proc/stat", "r")
    f_loadavg = open("/proc/loadavg", "r")

    while True:
        # proc.meminfo
        f_meminfo.seek(0)
        ts = int(time.time())
        for line in f_meminfo:
            m = re.match("(\w+):\s+(\d+)", line)
            if m:
                print "proc.meminfo.%s %d %s" % (m.group(1).lower(), ts, m.group(2))

        # proc.vmstat
        f_vmstat.seek(0)
        ts = int(time.time())
        for line in f_vmstat:
            m = re.match("(\w+)\s+(\d+)", line)
            if m:
                if m.group(1) in ("pgpgin", "pgpgout", "pswpin",
                                  "pswpout", "pgfault", "pgmajfault"):
                    print "proc.vmstat.%s %d %s" % (m.group(1), ts, m.group(2))

        # proc.stat
        f_stat.seek(0)
        ts = int(time.time())
        for line in f_stat:
            m = re.match("(\w+)\s+(.*)", line)
            if m:
                if m.group(1) == "cpu":
                    l = m.group(2).split()
                    print "proc.stat.cpu %d %s type=user" % (ts, l[0])
                    print "proc.stat.cpu %d %s type=nice" % (ts, l[1])
                    print "proc.stat.cpu %d %s type=system" % (ts, l[2])
                    print "proc.stat.cpu %d %s type=idle" % (ts, l[3])
                    print "proc.stat.cpu %d %s type=iowait" % (ts, l[4])
                    print "proc.stat.cpu %d %s type=irq" % (ts, l[5])
                    print "proc.stat.cpu %d %s type=softirq" % (ts, l[6])
                    if len(l) > 7:  # really old kernels don't have this field
                        print "proc.stat.cpu %d %s type=guest" % (ts, l[7])
                        if len(l) > 8:  # old kernels don't have this field
                            print "proc.stat.cpu %d %s type=guest_nice" % (ts, l[8])
                elif m.group(1) == "intr":
                    print "proc.stat.%s %d %s" % (m.group(1), ts, m.group(2).split()[0])
                elif m.group(1) == "ctxt":
                    print "proc.stat.%s %d %s" % (m.group(1), ts, m.group(2))

        f_loadavg.seek(0)
        ts = int(time.time())
        for line in f_loadavg:
            m = re.match("(\S+)\s+(\S+)\s+(\S+)\s+(\d+)/(\d+)\s+", line)
            if m:
                print "proc.loadavg.1min %d %s" % (ts, m.group(1))
                print "proc.loadavg.5min %d %s" % (ts, m.group(2))
                print "proc.loadavg.15min %d %s" % (ts, m.group(3))
                print "proc.loadavg.runnable %d %s" % (ts, m.group(4))
                print "proc.loadavg.total_threads %d %s" % (ts, m.group(5))

        sys.stdout.flush()
        time.sleep(interval)

if __name__ == "__main__":
    main()

