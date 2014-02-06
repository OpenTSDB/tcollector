#!/usr/bin/python
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
#
"""network interface stats for TSDB"""

import sys
import time
import re

from collectors.lib import utils

interval = 15 # seconds

# /proc/net/dev has 16 fields, 8 for receive and 8 for transmit,
# defined below.
# So we can aggregate up the total bytes, packets, etc
# we tag each metric with direction=in or =out
# and iface=

# The new naming scheme of network interfaces
# Lan-On-Motherboard interfaces
# em<port number>_< virtual function instance / NPAR Index >
#
# PCI add-in interfaces
# p<slot number>p<port number>_<virtual function instance / NPAR Index>


FIELDS = ("bytes", "packets", "errs", "dropped",
          "fifo.errs", "frame.errs", "compressed", "multicast",
          "bytes", "packets", "errs", "dropped",
          "fifo.errs", "collisions", "carrier.errs", "compressed")


def main():
    """ifstat main loop"""

    f_netdev = open("/proc/net/dev")
    utils.drop_privileges()

    # We just care about ethN and emN interfaces.  We specifically
    # want to avoid bond interfaces, because interface
    # stats are still kept on the child interfaces when
    # you bond.  By skipping bond we avoid double counting.
    while True:
        f_netdev.seek(0)
        ts = int(time.time())
        for line in f_netdev:
            m = re.match("\s+(eth\d+|em\d+_\d+/\d+|em\d+_\d+|em\d+|"
                         "p\d+p\d+_\d+/\d+|p\d+p\d+_\d+|p\d+p\d+):(.*)", line)
            if not m:
                continue
            intf = m.group(1)
            stats = m.group(2).split(None)
            def direction(i):
                if i >= 8:
                    return "out"
                return "in"
            for i in xrange(16):
                print ("proc.net.%s %d %s iface=%s direction=%s"
                       % (FIELDS[i], ts, stats[i], intf, direction(i)))

        sys.stdout.flush()
        time.sleep(interval)

if __name__ == "__main__":
    sys.exit(main())
