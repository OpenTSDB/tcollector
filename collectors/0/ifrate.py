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
Network interfaces detailed statistics for TSDB

This plugin tracks, for interfaces named in configuration file:

- packets/s, input
- bytes/s,   input
- errors/s,  input
- drops/s,   input
- packets/s, output
- bytes/s,   output
- errors/s,  output
- drops/s,   output
- collisions/s

Requirements :
- FreeBSD : netstat
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
    from collectors.etc import ifrate_conf
except ImportError:
    ifrate_conf = None

DEFAULT_COLLECTION_INTERVAL=15

signal_received = None
def handlesignal(signum, stack):
    global signal_received
    signal_received = signum

def main():
    """top main loop"""

    collection_interval=DEFAULT_COLLECTION_INTERVAL
    if(ifrate_conf):
        config = ifrate_conf.get_config()
        collection_interval=config['collection_interval']
        interfaces=config['interfaces']
        report_packets=config['report_packets']
        merge_err_in_out=config['merge_err_in_out']

    global signal_received

    signal.signal(signal.SIGTERM, handlesignal)
    signal.signal(signal.SIGINT, handlesignal)

    p_net = []
    intnum = 0
    try:
        if platform.system() == "FreeBSD":
            for intname in interfaces:
                p_net.append(subprocess.Popen(
                    ["netstat", "-I", intname, "-d", "-w", str(collection_interval)],
                    stdout=subprocess.PIPE,
                ))
                intnum+=1
        else:
            sys.exit(13) # we signal tcollector to not run us
    except OSError as e:
        if e.errno == errno.ENOENT:
            # it makes no sense to run this collector here
            sys.exit(13) # we signal tcollector to not run us
        raise

    timestamp = 0
    procnum = 0

    while signal_received is None:
        if (procnum >= intnum):
            procnum=0
        try:
            line = p_net[procnum].stdout.readline()
        except (IOError, OSError) as e:
            if e.errno in (errno.EINTR, errno.EAGAIN):
                break
            raise

        if not line:
            # end of the program, die
            break

        if (re.match("^[0-9 ]+$",line)):
            fields = line.split()
            if len(fields) == 9:
                if(procnum == 0):
                    timestamp = int(time.time())
                print("ifrate.byt.in %s %s int=%s" % (timestamp, int(fields[3])/collection_interval, interfaces[procnum]))
                print("ifrate.byt.out %s %s int=%s" % (timestamp, int(fields[6])/collection_interval, interfaces[procnum]))
                if(report_packets):
                    print("ifrate.pkt.in %s %s int=%s" % (timestamp, int(fields[0])/collection_interval, interfaces[procnum]))
                    print("ifrate.pkt.out %s %s int=%s" % (timestamp, int(fields[4])/collection_interval, interfaces[procnum]))
                if(merge_err_in_out):
                    print("ifrate.err %s %s int=%s" % (timestamp, (int(fields[1])+int(fields[5]))/collection_interval, interfaces[procnum]))
                    print("ifrate.drp %s %s int=%s" % (timestamp, (int(fields[2])+int(fields[8]))/collection_interval, interfaces[procnum]))
                else:
                    print("ifrate.err.in %s %s int=%s" % (timestamp, int(fields[1])/collection_interval, interfaces[procnum]))
                    print("ifrate.drp.in %s %s int=%s" % (timestamp, int(fields[2])/collection_interval, interfaces[procnum]))
                    print("ifrate.err.out %s %s int=%s" % (timestamp, int(fields[5])/collection_interval, interfaces[procnum]))
                    print("ifrate.drp.out %s %s int=%s" % (timestamp, int(fields[8])/collection_interval, interfaces[procnum]))
                print("ifrate.col %s %s int=%s" % (timestamp, int(fields[7])/collection_interval, interfaces[procnum]))

        # analyze next process
        procnum+=1

        sys.stdout.flush()

    if signal_received is None:
        signal_received = signal.SIGTERM
    try:
        for procnum in range(0, intnum):
            os.kill(p_net[procnum].pid, signal_received)
    except Exception:
        pass
    for procnum in range(0, intnum):
        p_net[procnum].wait()

    # If no line at all has been proceeded (wrong interface name ?), we signal tcollector to not run us
    if(timestamp == 0):
        exit(13)

if __name__ == "__main__":
    main()
