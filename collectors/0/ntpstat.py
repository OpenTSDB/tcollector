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
<<<<<<< HEAD
"""df disk space and inode counts for TSDB """
#
# dfstat.py
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

=======
"""ntp offset stats for TSDB """
#
# ntpstat.py
#
# ntp.offset             estimated offset
>>>>>>> ntp_collector

import os
import socket
import subprocess
import sys
import time


COLLECTION_INTERVAL = 60  # seconds

def main():
    """ntpstats main loop"""

<<<<<<< HEAD
    #find the primary server to get reference
    #should be the prefer on ntp.conf or the first server if not preferred.
    f = open('/etc/ntp.conf', 'r')
    server="none"
    for line in f:
        fields = line.split()
        if len(fields) <= 0 :
            continue
        if not line or fields[0] != 'server':
            continue
        if server == "none":
            server=fields[1]
        if line.find("prefer") > 0:
            server=fields[1]
    if server == "none":
        print >> sys.stderr, "Could not find a suitable time server to query, please check your ntp.conf"

    while True:
        ts = int(time.time())
	ntp_proc = subprocess.Popen(["ntpdate", "-q", server], stdout=subprocess.PIPE)
        stdout, _ = ntp_proc.communicate()
        if ntp_proc.returncode == 0:
            for line in stdout.split("\n"): # pylint: disable=E1103
                fields = line.split()
                # skip header/blank lines
                if not line or not fields[0].isdigit():
                    continue

                offset = fields[9]
		server = fields[7]
                print ("ntp.offset %d %s timeserver=%s"
                       % (ts, offset, server ))
                continue
=======
    while True:
        ts = int(time.time())
	ntp_proc = subprocess.Popen(["/usr/sbin/ntpdc", "-c", "loopinfo"], stdout=subprocess.PIPE)
        stdout, _ = ntp_proc.communicate()
        if ntp_proc.returncode == 0:
            for line in stdout.split("\n"): 
                if not line:
                    continue
                fields = line.split()
                if len(fields) <= 0: 
                    continue
                if fields[0] == "offset:":
                    offset=fields[1]    
                    continue
            print ("ntp.offset %d %s"
                    % (ts, offset))
>>>>>>> ntp_collector
        else:
            print >> sys.stderr, "ntpdate -q %s returned %r" % (server, ntp_proc.returncode)

        sys.stdout.flush()
        time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
    main()
