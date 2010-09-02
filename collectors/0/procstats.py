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

    while True:
        # proc.meminfo
        f = open("/proc/meminfo", "r")
        ts = int(time.time())
        for line in f:
            m = re.match("(\w+):\s+(\d+)", line)
            if m:
                print "proc.meminfo.%s %d %s" % (m.group(1).lower(), ts,
                                                 m.group(2))
        f.close()

        # proc.vmstat
        f = open("/proc/vmstat", "r")
        ts = int(time.time())
        for line in f:
            m = re.match("(\w+)\s+(\d+)", line)
            if m:
                if m.group(1) in ("pgpgin", "pgpgout", "pswpin",
                                  "pswpout", "pgfault", "pgmajfault"):
                    print "proc.vmstat.%s %d %s" % (m.group(1), ts, m.group(2))

        f.close()

        # proc.stat
        f = open("/proc/stat", "r")
        ts = int(time.time())
        for line in f:
            m = re.match("(\w+)\s+(.*)", line)
            if m:
                if m.group(1) == "cpu":
                    l = m.group(2).split()
                    print "proc.stat.cpu.user %d %s" % (ts, l[0])
                    print "proc.stat.cpu.nice %d %s" % (ts, l[1])
                    print "proc.stat.cpu.system %d %s" % (ts, l[2])
                    print "proc.stat.cpu.idle %d %s" % (ts, l[3])
                    print "proc.stat.cpu.iowait %d %s" % (ts, l[4])
                    print "proc.stat.cpu.irq %d %s" % (ts, l[5])
                    print "proc.stat.cpu.softirq %d %s" % (ts, l[6])
                    if len(l) > 7:  # really old kernels don't have this field
                        print "proc.stat.cpu.guest %d %s" % (ts, l[7])
                        if len(l) > 8:  # old kernels don't have this field
                            print "proc.stat.cpu.guest_nice %d %s" % (ts, l[8])
                elif m.group(1) == "intr":
                    print "proc.stat.%s %d %s" % (m.group(1), ts, m.group(2).split()[0])
                elif m.group(1) == "ctxt":
                    print "proc.stat.%s %d %s" % (m.group(1), ts, m.group(2))
        f.close()

        time.sleep(interval)


if __name__ == "__main__":
    main()

