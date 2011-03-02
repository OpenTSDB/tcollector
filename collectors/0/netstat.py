#!/usr/bin/python
# This file is part of tcollector.
# Copyright (C) 2011  StumbleUpon, Inc.
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

"""Socket allocation and network statistics for TSDB.

Metrics from /proc/net/sockstat:
  - net.sockstat.num_sockets: Number of sockets allocated (only TCP).
  - net.sockstat.num_timewait: Number of TCP sockets currently in
    TIME_WAIT state.
  - net.sockstat.sockets_inuse: Number of sockets in use (TCP/UDP/raw).
  - net.sockstat.num_orphans: Number of orphan TCP sockets (not attached
    to any file descriptor).
  - net.sockstat.memory: Memory allocated for this socket type (in bytes).
  - net.sockstat.ipfragqueues: Number of IP flows for which there are
    currently fragments queued for reassembly.
"""

import os
import pwd
import re
import resource
import sys
import time

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
    """Main loop"""
    drop_privileges()
    sys.stdin.close()

    interval = 15
    page_size = resource.getpagesize()

    try:
        sockstat = open("/proc/net/sockstat")
    except IOError, e:
        print >>sys.stderr, "Failed to open /proc/net/sockstat: %s" % e
        return 13  # Ask tcollector to not re-start us.

    # Note: up until v2.6.37-rc2 most of the values were 32 bits.
    # The first value is pretty useless since it accounts for some
    # socket types but not others.  So we don't report it because it's
    # more confusing than anything else and it's not well documented
    # what type of sockets are or aren't included in this count.
    regexp = re.compile("sockets: used \d+\n"
                        "TCP: inuse (?P<tcp_inuse>\d+) orphan (?P<orphans>\d+)"
                        " tw (?P<tw_count>\d+) alloc (?P<tcp_sockets>\d+)"
                        " mem (?P<tcp_pages>\d+)\n"
                        "UDP: inuse (?P<udp_inuse>\d+)"
                        # UDP memory accounting was added in v2.6.25-rc1
                        "(?: mem (?P<udp_pages>\d+))?\n"
                        # UDP-Lite (RFC 3828) was added in v2.6.20-rc2
                        "(?:UDPLITE: inuse (?P<udplite_inuse>\d+)\n)"
                        "RAW: inuse (?P<raw_inuse>\d+)\n"
                        "FRAG: inuse (?P<ip_frag_nqueues>\d+)"
                        " memory (?P<ip_frag_mem>\d+)\n")

    def print_sockstat(metric, value, tags=""):  # Note: tags must start with ' '
        if value is not None:
            print "net.sockstat.%s %d %s%s" % (metric, ts, value, tags)

    while True:
        ts = int(time.time())
        sockstat.seek(0)
        data = sockstat.read()
        m = re.match(regexp, data)
        if not m:
            print >>sys.stderr, "Cannot parse sockstat: %r" % data
            return 13

        # The difference between the first two values is the number of
        # sockets allocated vs the number of sockets actually in use.
        print_sockstat("num_sockets",   m.group("tcp_sockets"),   " type=tcp")
        print_sockstat("num_timewait",  m.group("tw_count"))
        print_sockstat("sockets_inuse", m.group("tcp_inuse"),     " type=tcp")
        print_sockstat("sockets_inuse", m.group("udp_inuse"),     " type=udp")
        print_sockstat("sockets_inuse", m.group("udplite_inuse"), " type=udplite")
        print_sockstat("sockets_inuse", m.group("raw_inuse"),     " type=raw")

        print_sockstat("num_orphans", m.group("orphans"))
        print_sockstat("memory", int(m.group("tcp_pages")) * page_size,
                       " type=tcp")
        if m.group("udp_pages") is not None:
          print_sockstat("memory", int(m.group("udp_pages")) * page_size,
                         " type=udp")
        print_sockstat("memory", m.group("ip_frag_mem"), " type=ipfrag")
        print_sockstat("ipfragqueues", m.group("ip_frag_nqueues"))

        sys.stdout.flush()
        time.sleep(interval)

if __name__ == "__main__":
    sys.exit(main())
