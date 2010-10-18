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
# dfstat.py
#
# df disk space and inode count statistics
#
# df.1kblocks.total      total size of fs
# df.1kblocks.used       blocks used
# df.1kblocks.available  blocks available
# df.inodes.total        number of inodes
# df.inodes.used        number of inodes
# df.inodes.free        number of inodes

# All metrics are tagged with mount= and fstype=
# This makes it easier to exclude stuff like
# tmpfs mounts from disk usage reports.

# Because tsdb does not like slashes in tags, slashes will
# be replaced by underscores in the mount= tag.  In theory
# this could cause problems if you have a mountpoint of
# "/foo/bar/" and "/foo_bar/".


import os
import socket
import subprocess
import sys
import time


COLLECTION_INTERVAL = 60  # seconds

def main():
    while True:
        ts = int(time.time())
        # 1kblocks
        p = subprocess.Popen(["df", "-PlTk"], stdout=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            for line in stdout.split("\n"):
                l = line.split()
                # skip header/blank lines
                if not line or not l[2].isdigit():
                    continue

                mount = l[6].replace('/', '_')
                print ("df.1kblocks.total %d %s mount=%s fstype=%s"
                       % (ts, l[2], mount, l[1]))
                print ("df.1kblocks.used %d %s mount=%s fstype=%s"
                       % (ts, l[3], mount, l[1]))
                print ("df.1kblocks.free %d %s mount=%s fstype=%s"
                       % (ts, l[4], mount, l[1]))
        else:
            print >>sys.stderr, "df -Pltk returned %r" % p.returncode

        ts = int(time.time())
        # inodes
        p = subprocess.Popen(["df", "-PlTi"], stdout=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            for line in stdout.split("\n"):
                l = line.split()
                if not line or not l[2].isdigit():
                    continue

                mount = l[6].replace('/', '_')
                print ("df.inodes.total %d %s mount=%s fstype=%s"
                       % (ts, l[2], mount, l[1]))
                print ("df.inodes.used %d %s mount=%s fstype=%s"
                       % (ts, l[3], mount, l[1]))
                print ("df.inodes.free %d %s mount=%s fstype=%s"
                       % (ts, l[4], mount, l[1]))
        else:
            print >>sys.stderr, "df -Plti returned %r" % p.returncode

        sys.stdout.flush()
        time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
    main()
