#!/usr/bin/env python
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
import sys
import time

from collectors.lib import utils

COLLECTION_INTERVAL = 60  # seconds

# File system types to ignore
FSTYPE_IGNORE = frozenset([
  "cgroup",
  "debugfs",
  "devtmpfs",
  "nfs",
  "rpc_pipefs",
  "rootfs",
])

def main():
  """dfstats main loop"""
  try:
    f_mounts = open("/proc/mounts", "r")
  except IOError as e:
    utils.err("error: can't open /proc/mounts: %s" % e)
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
      except ValueError as e:
        utils.err("error: can't parse line at /proc/mounts: %s" % e)
        continue

      if fs_spec == "none":
        continue
      elif fs_vfstype in FSTYPE_IGNORE or fs_vfstype.startswith("fuse."):
        continue
      # startswith(tuple) avoided to preserve support of Python 2.4
      elif fs_file.startswith("/dev") or fs_file.startswith("/sys") or \
            fs_file.startswith("/proc") or fs_file.startswith("/lib") or \
            fs_file.startswith("net:") or fs_file.startswith("/var/lib/kubelet"):
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
          devices.append([fs_spec, fs_file, fs_vfstype])
      else:
        devices.append([fs_spec, fs_file, fs_vfstype])


    for device in devices:
      fs_spec, fs_file, fs_vfstype = device
      try:
        r = os.statvfs(fs_file)
      except OSError as e:
        utils.err("can't get info for mount point: %s: %s" % (fs_file, e))
        continue

      used = r.f_blocks - r.f_bfree

      # conditional expression avoided to preserve support of Python 2.4
      # percent_used = 100 if r.f_blocks == 0 else used * 100.0 / r.f_blocks
      if r.f_blocks == 0:
          percent_used = 100
      else:
          percent_used = used * 100.0 / r.f_blocks

      print("df.bytes.total %d %s mount=%s fstype=%s"
            % (ts, r.f_frsize * r.f_blocks, fs_file, fs_vfstype))
      print("df.bytes.used %d %s mount=%s fstype=%s"
            % (ts, r.f_frsize * used, fs_file, fs_vfstype))
      print("df.bytes.percentused %d %s mount=%s fstype=%s"
            % (ts, percent_used, fs_file, fs_vfstype))
      print("df.bytes.free %d %s mount=%s fstype=%s"
            % (ts, r.f_frsize * r.f_bfree, fs_file, fs_vfstype))

      used = r.f_files - r.f_ffree

      # percent_used = 100 if r.f_files == 0 else used * 100.0 / r.f_files
      if r.f_files == 0:
          percent_used = 100
      else:
          percent_used = used * 100.0 / r.f_files

      print("df.inodes.total %d %s mount=%s fstype=%s"
            % (ts, r.f_files, fs_file, fs_vfstype))
      print("df.inodes.used %d %s mount=%s fstype=%s"
            % (ts, used, fs_file, fs_vfstype))
      print("df.inodes.percentused %d %s mount=%s fstype=%s"
            % (ts, percent_used,  fs_file, fs_vfstype))
      print("df.inodes.free %d %s mount=%s fstype=%s"
            % (ts, r.f_ffree, fs_file, fs_vfstype))

    sys.stdout.flush()
    time.sleep(COLLECTION_INTERVAL)


if __name__ == "__main__":
  sys.stdin.close()
  sys.exit(main())
