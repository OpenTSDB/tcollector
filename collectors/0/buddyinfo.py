#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2010-2013  The tcollector Authors.
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
"""memory buddy info fragmentation counters for TSDB"""

import sys
import time
import socket

from collectors.lib import utils

COLLECTION_INTERVAL = 1  # second

BUDDYINFO='/proc/buddyinfo'
METRIC='proc.meminfo.buddyinfo'
HOSTNAME=socket.gethostname()

def main():

    """buddyinfo main loop"""
    utils.drop_privileges()

    while True:
        epoch = int(time.time())
        try:
            with open(BUDDYINFO,'r') as buddyinfo:
                for line in buddyinfo.readlines(): 
                    zonedata = line.strip().split()
                    node = int(zonedata[1].strip(','))
                    zone = zonedata[3]
                    for order,val in enumerate(zonedata[4:]):
                        print("%s %s %s host=%s zone=%s node=%d order=%d" % (METRIC, epoch, val, HOSTNAME, zone, node, order))

        except IOError, e:
            utils.err("error: can't open %s: %s" % (BUDDYINFO,e))
            return 13 # Ask tcollector to not respawn us

        sys.stdout.flush()
        time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main())

