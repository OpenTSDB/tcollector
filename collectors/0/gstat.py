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
Disks detailed statistics for TSDB

This plugin tracks, for all FreeBSD disks:

- queue length
- read :
  - IOPS (operations/s)
  - block size (B)
  - throughput (B/s)
  - response time (ms)
- write :
  - IOPS (operations/s)
  - block size (B)
  - throughput (B/s)
  - response time (ms)
- delete (BIO_DELETE) :
  - IOPS (operations/s)
  - block size (B)
  - throughput (B/s)
  - response time (ms)
- other (BIO_FLUSH) :
  - IOPS (operations/s)
  - response time (ms)
- busy percentage

Requirements :
- FreeBSD : gstat (with the following patch https://bugs.freebsd.org/bugzilla/show_bug.cgi?id=212726)
'''

import errno
import sys
import time
import subprocess
import re
import signal
import os

from collectors.lib import utils

try:
    from collectors.etc import gstat_conf
except ImportError:
    gstat_conf = None

DEFAULT_COLLECTION_INTERVAL=15

signal_received = None
def handlesignal(signum, stack):
    global signal_received
    signal_received = signum

def main():
    """top main loop"""

    collection_interval=DEFAULT_COLLECTION_INTERVAL
    collection_filter=".*"
    if(gstat_conf):
        config = gstat_conf.get_config()
        collection_interval=config['collection_interval']
        collection_filter=config['collection_filter']

    global signal_received

    signal.signal(signal.SIGTERM, handlesignal)
    signal.signal(signal.SIGINT, handlesignal)

    try:
        p_gstat = subprocess.Popen(
            ["gstat", "-B", "-d", "-o", "-s", "-I"+str(collection_interval)+"s", "-f"+str(collection_filter)],
            stdout=subprocess.PIPE,
        )
    except OSError as e:
        if e.errno == errno.ENOENT:
            # it makes no sense to run this collector here
            sys.exit(13) # we signal tcollector to not run us
        raise

    timestamp = 0

    while signal_received is None:
        try:
            line = p_gstat.stdout.readline()
        except (IOError, OSError) as e:
            if e.errno in (errno.EINTR, errno.EAGAIN):
                break
            raise

        if not line:
            # end of the program, die
            break

        if (not re.match("^ *[0-9]",line)):
            timestamp = int(time.time())
            continue

        fields = line.split()

        print("disk.queue %s %s disk=%s" % (timestamp, fields[0], fields[17]))
        print("disk.ops.read %s %s disk=%s" % (timestamp, fields[2], fields[17]))
        print("disk.b.read %s %d disk=%s" % (timestamp, float(fields[3])*1024, fields[17]))
        print("disk.bps.read %s %d disk=%s" % (timestamp, float(fields[4])*1024, fields[17]))
        print("disk.ms.read %s %s disk=%s" % (timestamp, float(fields[5]), fields[17]))
        print("disk.ops.write %s %s disk=%s" % (timestamp, fields[6], fields[17]))
        print("disk.b.write %s %d disk=%s" % (timestamp, float(fields[7])*1024, fields[17]))
        print("disk.bps.write %s %d disk=%s" % (timestamp, float(fields[8])*1024, fields[17]))
        print("disk.ms.write %s %s disk=%s" % (timestamp, float(fields[9]), fields[17]))
        print("disk.ops.delete %s %s disk=%s" % (timestamp, fields[10], fields[17]))
        print("disk.b.delete %s %d disk=%s" % (timestamp, float(fields[11])*1024, fields[17]))
        print("disk.bps.delete %s %d disk=%s" % (timestamp, float(fields[12])*1024, fields[17]))
        print("disk.ms.delete %s %s disk=%s" % (timestamp, float(fields[13]), fields[17]))
        print("disk.ops.other %s %s disk=%s" % (timestamp, fields[14], fields[17]))
        print("disk.ms.other %s %s disk=%s" % (timestamp, float(fields[15]), fields[17]))
        print("disk.busy %s %s disk=%s" % (timestamp, fields[16], fields[17]))

        sys.stdout.flush()
        
    if signal_received is None:
        signal_received = signal.SIGTERM
    try:
        os.kill(p_gstat.pid, signal_received)
    except Exception:
        pass
    p_gstat.wait()

if __name__ == "__main__":
    main()
