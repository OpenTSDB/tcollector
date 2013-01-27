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
"""network interface stats for TSDB"""

import os
import pwd
import sys
import time
import socket
import re


# /proc/net/dev has 16 fields, 8 for receive and 8 for xmit
# The fields we care about are defined here.  The
# ones we want to skip we just leave empty.
# So we can aggregate up the total bytes, packets, etc
# we tag each metric with direction=in or =out
# and iface=

FIELDS = ("bytes", "packets", "errs", "dropped",
           None, None, None, None,)

# If we're running as root and this user exists, we'll drop privileges.
USER = "nobody"


def drop_privileges():
    """Drops privileges if running as root."""
    try:
        ent = pwd.getpwnam(USER)
    except KeyError:
        return

    if os.getuid() != 0:
        return

    os.setgid(ent.pw_gid)
    os.setuid(ent.pw_uid)

def main():
    """ifstat main loop"""
    interval = 15

    f_netdev = open("/proc/net/dev", "r")
    drop_privileges()

    # We just care about ethN interfaces.  We specifically
    # want to avoid bond interfaces, because interface
    # stats are still kept on the child interfaces when
    # you bond.  By skipping bond we avoid double counting.
    while True:
        f_netdev.seek(0)
        ts = int(time.time())
        for line in f_netdev:
            m = re.match("\s+(eth\d+):(.*)", line)
            if not m:
                continue
            stats = m.group(2).split(None)
            for i in range(8):
                if FIELDS[i]:
                    print ("proc.net.%s %d %s iface=%s direction=in"
                           % (FIELDS[i], ts, stats[i], m.group(1)))
                    print ("proc.net.%s %d %s iface=%s direction=out"
                           % (FIELDS[i], ts, stats[i+8], m.group(1)))

        sys.stdout.flush()
        time.sleep(interval)

if __name__ == "__main__":
    main()

