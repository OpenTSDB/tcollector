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
                    (c_user,c_nice,c_system,c_idle,c_iowait,c_irq,c_softirq,
                     c_guest,c_guest_nice)=m.group(2).split()
                    print "proc.stat.cpu.user %d %s" % (ts, c_user)
                    print "proc.stat.cpu.nice %d %s" % (ts, c_nice)
                    print "proc.stat.cpu.system %d %s" % (ts, c_system)
                    print "proc.stat.cpu.idle %d %s" % (ts, c_idle)
                    print "proc.stat.cpu.iowait %d %s" % (ts, c_iowait)
                    print "proc.stat.cpu.irq %d %s" % (ts, c_irq)
                    print "proc.stat.cpu.softirq %d %s" % (ts, c_softirq)
                    print "proc.stat.cpu.guest %d %s" % (ts, c_guest)
                    print "proc.stat.cpu.guest_nice %d %s" % (ts, c_guest_nice)
                elif m.group(1) == "intr":
                    print "proc.stat.%s %d %s" % (m.group(1), ts, m.group(2).split()[0])
                elif m.group(1) == "ctxt":
                    print "proc.stat.%s %d %s" % (m.group(1), ts, m.group(2))
        f.close()

        time.sleep(interval)


if __name__ == "__main__":
    main()

