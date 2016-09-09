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

'''
CPU detailed statistics for TSDB

This plugin tracks, for all CPUs:

- user %
- nice %
- system %
- interrupt %
- idle %

Requirements :
- FreeBSD : top
- Linux : mpstat

In addition, for FreeBSD, it reports :
- load average (1m, 5m, 15m)
- number of processes (total, running, sleeping)
'''

import errno
import sys
import time
import subprocess
import re
import signal
import os
import platform

from collectors.lib import utils

try:
    from collectors.etc import sysload_conf
except ImportError:
    sysload_conf = None

DEFAULT_COLLECTION_INTERVAL=15

signal_received = None
def handlesignal(signum, stack):
    global signal_received
    signal_received = signum

def main():
    """top main loop"""

    collection_interval=DEFAULT_COLLECTION_INTERVAL
    if(sysload_conf):
        config = sysload_conf.get_config()
        collection_interval=config['collection_interval']

    global signal_received

    signal.signal(signal.SIGTERM, handlesignal)
    signal.signal(signal.SIGINT, handlesignal)

    try:
        if platform.system() == "FreeBSD":
            p_top = subprocess.Popen(
                ["top", "-u", "-t", "-I", "-P", "-n", "-s"+str(collection_interval), "-d"+str((365*24*3600)/collection_interval)],
                stdout=subprocess.PIPE,
            )
        else:
            p_top = subprocess.Popen(
                ["mpstat", "-P", "ALL", str(collection_interval)],
                stdout=subprocess.PIPE,
            )
    except OSError, e:
        if e.errno == errno.ENOENT:
            # it makes no sense to run this collector here
            sys.exit(13) # we signal tcollector to not run us
        raise

    timestamp = 0

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

        fields = re.sub(r"%( [uni][a-z]+,?)?| AM | PM ", "", line).split()
        if len(fields) <= 0:
            continue

        if (((fields[0] == "CPU") or (re.match("[0-9][0-9]:[0-9][0-9]:[0-9][0-9]",fields[0]))) and (re.match("[0-9]+:?",fields[1]))):
            if(fields[1] == "0"):
                timestamp = int(time.time())
            cpuid=fields[1].replace(":","")
            cpuuser=fields[2]
            cpunice=fields[3]
            cpusystem=fields[4]
            cpuinterrupt=fields[6]
            cpuidle=fields[-1]
            print ("cpu.usr %s %s cpu=%s" % (timestamp, cpuuser, cpuid))
            print ("cpu.nice %s %s cpu=%s" % (timestamp, cpunice, cpuid))
            print ("cpu.sys %s %s cpu=%s" % (timestamp, cpusystem, cpuid))
            print ("cpu.irq %s %s cpu=%s" % (timestamp, cpuinterrupt, cpuid))
            print ("cpu.idle %s %s cpu=%s" % (timestamp, cpuidle, cpuid))
        
        elif (re.match("(.* load averages: *)",line)):
            timestamp = int(time.time())
            fields = re.sub(r".* load averages: *|,", "", line).split()
            print ("load.1m %s %s" % (timestamp, fields[0]))
            print ("load.5m %s %s" % (timestamp, fields[1]))
            print ("load.15m %s %s" % (timestamp, fields[2]))

        elif (re.match("[0-9]+ processes:",line)):
            fields = re.sub(r",", "", line).split()
            running=0
            sleeping=0
            for i in range(len(fields)):
                if(fields[i] == "running"):
                    running=fields[i-1]
                if(fields[i] == "sleeping"):
                    sleeping=fields[i-1]
            print ("ps.all %s %s" % (timestamp, fields[0]))
            print ("ps.run %s %s" % (timestamp, running))
            print ("ps.sleep %s %s" % (timestamp, sleeping))

    if signal_received is None:
        signal_received = signal.SIGTERM
    try:
        os.kill(p_top.pid, signal_received)
    except Exception:
        pass
    p_top.wait()

if __name__ == "__main__":
    main()
