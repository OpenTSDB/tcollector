#!/usr/bin/python
# This file is part of tcollector.
# Copyright (C) 2010-2013  The tcollector Authors.
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

from lib import utils

COLLECTION_INTERVAL = 60  # seconds

# File system types to ignore
FSTYPE_IGNORE = frozenset([
  "cgroup",
  "debugfs",
  "devtmpfs",
  "rpc_pipefs",
  "rootfs",
])


def err(msg):
  print >> sys.stderr, msg


def main():
  """dfstats main loop"""
  try:
    f_mounts = open("/proc/mounts", "r")
  except IOError, e:
    err("error: can't open /proc/mounts: %s" % e)
    return 13 # Ask tcollector to not respawn us

  utils.drop_privileges()

  while True:
    devices = []
    f_mounts.seek(0)
    ts = int(time.time())

    for line in f_mounts:
      # Docs come from the fstab(5)
      # fs_spec     # Mounted block special device or remote filesystem
      # fs_file     # Mount point
      # fs_vfstype  # File system type
      # fs_mntops   # Mount options
      # fs_freq     # Dump(8) utility flags
      # fs_passno   # Order in which filesystem checks are done at reboot time
      try:
        fs_spec, fs_file, fs_vfstype, fs_mntops, fs_freq, fs_passno = line.split(None)
      except ValueError, e:
        err("error: can't parse line at /proc/mounts: %s" % e)
        continue

      if fs_spec == "none":
        continue
      if fs_vfstype in FSTYPE_IGNORE or fs_vfstype.startswith("fuse."):
        continue
      if fs_file.startswith(("/dev", "/sys", "/proc", "/lib")):
        continue

      # keep /dev/xxx device with shorter fs_file (remove mount binds)
      device_found = False
      if fs_spec.startswith("/dev"):
        for device in devices:
          if fs_spec == device[0]:
            device_found = True
            if len(fs_file) < len(device[1]):
              device[1] = fs_file
            break
        if not device_found:
          devices.append((fs_spec, fs_file, fs_vfstype))
      else:
        devices.append((fs_spec, fs_file, fs_vfstype))


    for device in devices:
      fs_spec, fs_file, fs_vfstype = device
      try:
        r = os.statvfs(fs_file)
      except OSError, e:
        err("error: can't get info for mount point: %s" % fs_file)
        continue

      used_blocks = (r.f_blocks - r.f_bfree)
      print("df.bytes.total %d %s mount=%s fstype=%s"
            % (ts, r.f_frsize * r.f_blocks, fs_file, fs_vfstype))
      print("df.bytes.used %d %s mount=%s fstype=%s"
            % (ts, r.f_frsize * used_blocks, fs_file,
               fs_vfstype))
      print("df.bytes.free %d %s mount=%s fstype=%s"
            % (ts, r.f_frsize * r.f_bfree, fs_file, fs_vfstype))
      # usage is calculated by using the non-reserved notion of free space (ie f_bavail instead f_bfree)
      print("df.bytes.usage %d %f mount=%s fstype=%s"
            % (ts, (used_blocks  * 100.0) / (used_blocks + r.f_bavail), fs_file, fs_vfstype))

      used_inodes = r.f_files - r.f_ffree
      print("df.inodes.total %d %s mount=%s fstype=%s"
            % (ts, r.f_files, fs_file, fs_vfstype))
      print("df.inodes.used %d %s mount=%s fstype=%s"
            % (ts, used_inodes, fs_file, fs_vfstype))
      print("df.inodes.free %d %s mount=%s fstype=%s"
            % (ts, r.f_ffree, fs_file, fs_vfstype))
      # see note above
      print("df.inodes.usage %d %f mount=%s fstype=%s"
            % (ts, (used_inodes * 100.0) / (used_inodes + r.f_favail), fs_file, fs_vfstype))


    sys.stdout.flush()
    time.sleep(COLLECTION_INTERVAL)


if __name__ == "__main__":
  sys.stdin.close()
  sys.exit(main())
