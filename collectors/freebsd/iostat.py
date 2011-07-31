#!/usr/bin/env python
# iostat monitoring for FreeBSD.
#
# By: Sean Rees <sean@rees.us>
# July 2011.
#
"""Collect IO data."""

import fcntl
import os
import subprocess
import sys
import time

COLLECTION_INTERVAL = 15  # seconds

def iostat():
    proc = subprocess.Popen(["/usr/sbin/iostat", "-Ix",
                            str(COLLECTION_INTERVAL)],
                            stdout=subprocess.PIPE,
                            bufsize=0)

    # Set stdout to be non-blocking.
    fd = proc.stdout.fileno()
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

    return proc

def readlines(stdout):
    while True:
        try:
            data = stdout.read(1024)
            return data.split('\n')
        except IOError:
            pass

        # Wait around a bit.
        time.sleep(1)

def main():
    """main loop"""

    proc = iostat()

    # Purge the first output, since it's worthless.
    readlines(proc.stdout)

    while True:
        ts = int(time.time())

        data = readlines(proc.stdout)

        for line in data:
            line = line.strip()
            if line == 'extended device statistics':
                # Good sign, but ignore.
                continue
            elif line.startswith('device'):
                # Also ignore.
                continue
            elif not line:
                # Empty.
                continue
            else:
                info = line.split()
                dev = info[0]

                print "iostat.reads %d %s device=%s" % (ts, info[1], dev)
                print "iostat.writes %d %s device=%s" % (ts, info[2], dev)
                print "iostat.read_kb %d %s device=%s" % (ts, info[3], dev)
                print "iostat.write_kb %d %s device=%s" % (ts, info[4], dev)
                print "iostat.wait %d %s device=%s" % (ts, info[5], dev)
                print "iostat.svc_time %d %s device=%s" % (ts, info[6], dev)
                print "iostat.busy %d %s device=%s" % (ts, info[7], dev)

        sys.stdout.flush()
        time.sleep(COLLECTION_INTERVAL)

    proc.terminate()
    proc.wait()

if __name__ == "__main__":
    main()
