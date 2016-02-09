#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2012  The tcollector Authors.
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

import errno
import sys
import time
import subprocess
import re
import signal
import os


'''
CPU detailed statistics for TSDB

This plugin tracks, for all CPUs:

- user %
- nice %
- system %
- interrupt %
- idle %
'''

signal_received = None
def handlesignal(signum, stack):
    global signal_received
    signal_received = signum

def main():
    """top main loop"""
    global signal_received

    signal.signal(signal.SIGTERM, handlesignal)
    signal.signal(signal.SIGINT, handlesignal)

    try:
        p_top = subprocess.Popen(
            ["top", "-t", "-I", "-P", "-n", "-s15", "-d40320"],
            stdout=subprocess.PIPE,
        )
    except OSError, e:
        if e.errno == errno.ENOENT:
            # it makes no sense to run this collector here
            sys.exit(13) # we signal tcollector to not run us
        raise

    while signal_received is None:
        try:
            line = p_top.stdout.readline()
        except (IOError, OSError), e:
            if e.errno in (errno.EINTR, errno.EAGAIN):
                break
            raise

        if not line:
            # end of the program, die
            break

        fields = line.split()
        if len(fields) <= 0:
            continue

        if fields[0] == "CPU":
            timestamp = int(time.time())
            cpuid=fields[1][:-1]
            cpuuser=fields[2][:-1]
            cpunice=fields[4][:-1]
            cpusystem=fields[6][:-1]
            cpuinterrupt=fields[8][:-1]
            cpuidle=fields[10][:-1]
            print ("cpu.user %s %s cpuname=%s" % (timestamp, cpuuser, cpuid))
            print ("cpu.nice %s %s cpuname=%s" % (timestamp, cpunice, cpuid))
            print ("cpu.system %s %s cpuname=%s" % (timestamp, cpusystem, cpuid))
            print ("cpu.interrupt %s %s cpuname=%s" % (timestamp, cpuinterrupt, cpuid))
            print ("cpu.idle %s %s cpuname=%s" % (timestamp, cpuidle, cpuid))

    if signal_received is None:
        signal_received = signal.SIGTERM
    try:
        os.kill(p_top.pid, signal_received)
    except Exception:
        pass
    p_top.wait()

if __name__ == "__main__":
    main()

