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
#
"""network interface stats for TSDB"""

import sys
import time
import re

from collectors.lib import utils

COLLECTION_INTERVAL = 15  # seconds

# /proc/net/dev has 16 fields, 8 for receive and 8 for transmit, defined below.
# So we can aggregate up the total bytes, packets, etc
# we tag each metric with direction=in or =out
# and iface=

# The new naming scheme of network interfaces
# Lan-On-Motherboard interfaces
# em<port number>_< virtual function instance / NPAR Index >
#
# PCI add-in interfaces
# p<slot number>p<port number>_<virtual function instance / NPAR Index>

FIELDS = ("bytes", "packets", "errs", "drop", "fifo", "frame", "compressed", "multicast",  # receive
          "bytes", "packets", "errs", "drop", "fifo", "colls", "carrier", "compressed")  # transmit

PATTERN = re.compile(r'''
     \s*                                     # Leading whitespace
     (?P<interface>\w+):\s+                  # Network interface name followed by colon and whitespace

     (?P<receive_bytes>\d+)\s+               # Receive bytes
     (?P<receive_packets>\d+)\s+             # Receive packets
     (?P<receive_errs>\d+)\s+                # Receive errors
     (?P<receive_drop>\d+)\s+                # Receive dropped packets
     (?P<receive_fifo>\d+)\s+                # Receive FIFO errors
     (?P<receive_frame>\d+)\s+               # Receive frame errors
     (?P<receive_compressed>\d+)\s+          # Receive compressed packets
     (?P<receive_multicast>\d+)\s+           # Receive multicast packets
     (?P<transmit_bytes>\d+)\s+              # Transmit bytes
     (?P<transmit_packets>\d+)\s+            # Transmit packets
     (?P<transmit_errs>\d+)\s+               # Transmit errors
     (?P<transmit_drop>\d+)\s+               # Transmit dropped packets
     (?P<transmit_fifo>\d+)\s+               # Transmit FIFO errors
     (?P<transmit_colls>\d+)\s+              # Transmit collisions
     (?P<transmit_carrier>\d+)\s+            # Transmit carrier errors
     (?P<transmit_compressed>\d+)\s*         # Transmit compressed packets
 ''', re.VERBOSE)


def main():
    """ifstat main loop"""

    f_netdev = open("/proc/net/dev", encoding='utf-8')
    utils.drop_privileges()

    # We just care about ethN and emN interfaces. We specifically want to avoid
    # bond interfaces, because interface stats are still kept on the child interfaces
    # when you bond. By skipping bond we avoid double counting.
    while True:
        f_netdev.seek(0)
        time_stamp = int(time.time())
        for line in f_netdev:
            match = PATTERN.match(line)

            if not match:
                continue

            if match.group('interface').startswith('bond'):  # avoid bond interface
                continue

            for i in range(16):
                print(f"proc.net.{FIELDS[i]} {time_stamp} {match.group(i+2)} "
                      f"iface={match.group('interface')} direction={'in' if i < 8 else 'out'}")

        sys.stdout.flush()
        time.sleep(COLLECTION_INTERVAL)


if __name__ == "__main__":
    sys.exit(main())
