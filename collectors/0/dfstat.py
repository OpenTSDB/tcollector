#!/usr/bin/python
# This file is part of tcollector.
# Copyright (C) 2013  The tcollector Authors.
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
import sys
import time

from collectors.lib import utils

COLLECTION_INTERVAL = 60  # seconds


def err(msg):
  print >>sys.stderr, msg

def main():
    """dfstats main loop"""
    try:
        f_mounts = open("/proc/mounts", "r")
    except IOError, e:
        err("error: can't open /proc/mounts: %s" % e)
        return 13 # Ask tcollector to not respawn us


    utils.drop_privileges()

    while True:
        f_mounts.seek(0)
        ts = int(time.time())

        for line in f_mounts:
            """
            Docs come from the fstab(5)
            fs_spec     # Mounted block special device or remote filesystem
            fs_file     # Mount point
            fs_vfstype  # File system type
            fs_mntops   # Mount options
            fs_freq     # Dump(8) utility flags
            fs_passno   # Order in which filesystem checks are done at reboot time
            """
            fs_spec, fs_file, fs_vfstype, fs_mntops, fs_freq, fs_passno = line.split(None)

            # Skip mounts/types we don't care about.
            # Most of this stuff is of type tmpfs, but we don't want
            # to blacklist all tmpfs since sometimes it's used for
            # active filesystems (/var/run, /tmp) that we do want to track.
            if fs_vfstype in ("debugfs", "devtmpfs", "rpc_pipefs", "rootfs"):
                continue
            if fs_file.startswith("/dev", "/sys", "/proc", "/lib"):
                continue

            r = os.statvfs(fs_file)

            print("df.bytes.total %d %s mount=%s fstype=%s"
                  % (ts, r.f_frsize * r.f_blocks, fs_file, fs_vfstype))
            print("df.bytes.used %d %s mount=%s fstype=%s"
                  % (ts, r.f_frsize * (r.f_blocks - r.f_bfree), fs_file,
                     fs_vfstype))
            print("df.bytes.free %d %s mount=%s fstype=%s"
                  % (ts, r.f_frsize * r.f_bfree, fs_file, fs_vfstype))

            print("df.inodes.total %d %s mount=%s fstype=%s"
                  % (ts, r.f_files, fs_file, fs_vfstype))
            print("df.inodes.used %d %s mount=%s fstype=%s"
                  % (ts, (r.f_files - r.f_ffree), fs_file, fs_vfstype))
            print("df.inodes.free %d %s mount=%s fstype=%s"
                  % (ts, r.f_ffree, fs_file, fs_vfstype))

    sys.stdout.flush()
    time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main(sys.argv))
