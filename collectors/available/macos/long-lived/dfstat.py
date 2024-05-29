#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2014  The tcollector Authors.
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
"""disk space and inode counts for TSDB """
#
# dfstat.py
#
# df.bytes.total        total size of fs
# df.bytes.used         bytes used
# df.bytes.percentused  percentage of bytes used
# df.bytes.free         bytes free
# df.inodes.total       number of inodes
# df.inodes.used        number of inodes
# df.inodes.percentused percentage of inodes used
# df.inodes.free        number of inodes

# All metrics are tagged with mount= and fstype=
# This makes it easier to exclude stuff like
# tmpfs mounts from disk usage reports.

import os
import re
import sys
import time
import subprocess

from collectors.lib import utils

COLLECTION_INTERVAL = 60  # seconds

# File system types to ignore
FSTYPE_IGNORE = frozenset([
    "devfs",
    "autofs",
    "map",
    "smbfs",
])

PATTERN = re.compile(r'^(?P<device>\S*)\s+on\s+(?P<mount_point>\S*)\s+\((?P<fs_type>\S*),\s+(?P<options>[^)]+\))$', re.VERBOSE)


def main():
    """dfstats main loop"""
    utils.drop_privileges()

    while True:
        ts = int(time.time())

        output = subprocess.check_output(["mount"]).decode("utf-8")

        for line in output.splitlines():
            match = PATTERN.match(line)

            if match:
                fs_type = match.group('fs_type')
                if fs_type in FSTYPE_IGNORE:
                    continue

                mount_point = match.group('mount_point')
                try:
                    r = os.statvfs(mount_point)
                except OSError as e:
                    utils.err("can't get info for mount point: %s: %s" % (mount_point, e))
                    print("can't get info for mount point: %s: %s" % (mount_point, e))
                    continue

                used = r.f_blocks - r.f_bfree

                if r.f_blocks == 0:
                    percent_used = 100
                else:
                    percent_used = used * 100.0 / (used + r.f_bavail)

                print(f"df.bytes.total {ts} {r.f_frsize * r.f_blocks} mount={mount_point} fstype={fs_type}")
                print(f"df.bytes.used {ts} {r.f_frsize * used} mount={mount_point} fstype={fs_type}")
                print(f"df.bytes.percentused {ts} {percent_used:.2f} mount={mount_point} fstype={fs_type}")
                print(f"df.bytes.free {ts} {r.f_frsize * r.f_bavail} mount={mount_point} fstype={fs_type}")

                used = r.f_files - r.f_ffree

                if r.f_files == 0:
                    percent_used = 100
                else:
                    percent_used = used * 100.0 / r.f_files

                print(f"df.inodes.total {ts} {r.f_files} mount={mount_point} fstype={fs_type}")
                print(f"df.inodes.used {ts} {used} mount={mount_point} fstype={fs_type}")
                print(f"df.inodes.percentused {ts} {percent_used:.2f} mount={mount_point} fstype={fs_type}")
                print(f"df.inodes.free {ts} {r.f_ffree} mount={mount_point} fstype={fs_type}")

        sys.stdout.flush()
        time.sleep(COLLECTION_INTERVAL)


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main())
