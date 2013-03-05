#!/usr/bin/env python
# Network monitoring for FreeBSD.
#
# By: Sean Rees <sean@rees.us>
# December 2011.
#
"""Get network stats."""

import re
import subprocess
import sys
import time

def netstat():
    proc = subprocess.Popen(["/usr/bin/netstat", "-i", "-b", "-d"], stdout=subprocess.PIPE)
    output = proc.communicate()[0]

    if output:
        lines = output.split("\n")

        # We skip the header line.
        return lines[1:]

    # TODO: better error handling.
    return []

def main(argv):
    """main loop"""

    ts = int(time.time())

    netstat_lines = netstat()

    for line in netstat_lines:
        if not line:
            continue

        fields = line.split()

        ifname = fields[0]
        mtu = fields[1]

        # We don't care about these. Might want to add lo0 to this.
        if ifname in ['usbus', 'plip0']:
            continue

        # For the moment, we only care about the interface as a whole.
        if "Link#" not in fields[2]:
            continue

        # Now comes the fun part. netstat does not return a complete
        # table; some fields (e.g; "address") are missing on virtual
        # interfaces. So we must pick off the fields from the end.
        ipkts = fields[-9]
        ierrs = fields[-8]
        idrop = fields[-7]
        ibytes = int(fields[-6])

        opkts = fields[-5]
        oerrs = fields[-4]
        obytes = int(fields[-3])
        coll = fields[-2]
        odrop = fields[-1]

        print "ifstat.mtu %d %s if=%s" % (ts, mtu, ifname)
        print "ifstat.collisions %d %s if=%s" % (ts, coll, ifname)
        print "ifstat.packets_in %d %s if=%s" % (ts, ipkts, ifname)
        print "ifstat.errors_in %d %s if=%s" % (ts, ierrs, ifname)
        print "ifstat.drops_in %d %s if=%s" % (ts, idrop, ifname)
        print "ifstat.kbits_in %d %s if=%s" % (ts, (ibytes / 1024) * 8, ifname)
        print "ifstat.packets_out %d %s if=%s" % (ts, opkts, ifname)
        print "ifstat.errors_out %d %s if=%s" % (ts, oerrs, ifname)
        print "ifstat.drops_out %d %s if=%s" % (ts, odrop, ifname)
        print "ifstat.kbits_out %d %s if=%s" % (ts, (obytes / 1024) * 8, ifname)

        sys.stdout.flush()

if __name__ == "__main__":
    sys.exit(main(sys.argv))
