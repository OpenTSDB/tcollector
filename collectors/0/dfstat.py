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
"""disk space and inode counts for TSDB """
#
# dfstat.py
#
# df.bytes.total        total size of fs
# df.bytes.used         bytes used
# df.bytes.free         bytes free
# df.inodes.total       number of inodes
# df.inodes.used        number of inodes
# df.inodes.free        number of inodes

# All metrics are tagged with mount= and fstype=
# This makes it easier to exclude stuff like
# tmpfs mounts from disk usage reports.


import os
import pwd
import socket
import sys
import time
import re


COLLECTION_INTERVAL = 60  # seconds

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
    """dfstats main loop"""

    f_mounts = open("/proc/mounts", "r")

    drop_privileges()
    while True:
        ts = int(time.time())

        f_mounts.seek(0)
        for line in f_mounts:
            fs_spec, fs_file, fs_vfstype, fs_mntops, fs_freq, fs_passno = line.split()

            # Skip mounts/types we don't care about.
            # Most of this stuff is of type tmpfs, but we don't
            # want to blacklist all tmpfs since sometimes it's
            # used for active filesystems (/var/run, /tmp)
            # that we do want to track.
            if fs_vfstype in ("debugfs", "devtmpfs", "rpc_pipefs", "rootfs"):
                continue
            if fs_file.startswith("/dev"):
                continue
            if fs_file.startswith("/sys"):
                continue
            if fs_file.startswith("/proc"):
                continue
            if fs_file.startswith("/lib"):
                continue

            r = os.statvfs(fs_file)

            print ("df.bytes.total %d %s mount=%s fstype=%s"
                    % (ts, r.f_frsize * r.f_blocks, fs_file, fs_vfstype))
            print ("df.bytes.used %d %s mount=%s fstype=%s"
                    % (ts, r.f_frsize * (r.f_blocks - r.f_bfree), fs_file, fs_vfstype))
            print ("df.bytes.free %d %s mount=%s fstype=%s"
                    % (ts, r.f_frsize * r.f_bfree, fs_file, fs_vfstype))

            print ("df.inodes.total %d %s mount=%s fstype=%s"
                    % (ts, r.f_files, fs_file, fs_vfstype))
            print ("df.inodes.used %d %s mount=%s fstype=%s"
                    % (ts, (r.f_files - r.f_ffree), fs_file, fs_vfstype))
            print ("df.inodes.free %d %s mount=%s fstype=%s"
                    % (ts, r.f_ffree, fs_file, fs_vfstype))

        sys.stdout.flush()
        time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
    main()
