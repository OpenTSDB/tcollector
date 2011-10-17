#!/usr/bin/env python
# Swap monitoring for FreeBSD.
#
# By: Sean Rees <sean@rees.us>
# July 2011.
#
"""Get swap data."""

import re
import subprocess
import sys
import time

COLLECTION_INTERVAL = 15  # seconds

def swapinfo():
    proc = subprocess.Popen(["/usr/sbin/swapinfo"], stdout=subprocess.PIPE)
    output = proc.communicate()[0]

    if output:
        lines = output.split("\n")

        # We skip the header line.
        return lines[1:]

    # TODO: better error handling.
    return []

def print_swaplines(ts, device, size, used, avail):
    # We multiply by 1024 here to give us bytes; which is what sysctl
    # reports (either by hook or by crook as in the case of some of the
    # synthesized vm.* variables.
    print "swap.size %d %s device=%s" % (ts, int(size)*1024, device)
    print "swap.used %d %s device=%s" % (ts, int(used)*1024, device)
    print "swap.avail %d %s device=%s" % (ts, int(avail)*1024, device)

def main():
    """main loop"""

    while True:
        ts = int(time.time())

        swapinfo_lines = swapinfo()

        for line in swapinfo_lines:
            if not line:
                continue

            device, size, used, avail, capacity = re.split("\s+", line)

            device = device.replace("/dev/", "")

            print_swaplines(ts, device, size, used, avail)

        sys.stdout.flush()
        time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
    main()
