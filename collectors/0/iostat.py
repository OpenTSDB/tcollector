#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2010  The tcollector Authors.
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

"""iostat statistics for TSDB"""

# data is from /proc/diskstats

# Calculate disk statistics.  We handle 2.6 kernel output only, both
# pre-2.6.25 and post (which added back per-partition disk stats).
# (diskstats output significantly changed from 2.4).
# The fields (from iostats.txt) are mainly rate counters
# (either number of operations or number of milliseconds doing a
# particular operation), so let's just let TSD do the rate
# calculation for us.
#
# /proc/diskstats has 11 stats for a given device
# these are all rate counters except ios_in_progress
# .read_requests       Number of reads completed
# .read_merged         Number of reads merged
# .read_sectors        Number of sectors read
# .msec_read           Time in msec spent reading
# .write_requests      Number of writes completed
# .write_merged        Number of writes merged
# .write_sectors       Number of sectors written
# .msec_write          Time in msec spent writing
# .ios_in_progress     Number of I/O operations in progress
# .msec_total          Time in msec doing I/O
# .msec_weighted_total Weighted time doing I/O (multiplied by ios_in_progress)

# in 2.6.25 and later, by-partition stats are reported same as disks
# in 2.6 before 2.6.25, partitions have 4 stats per partition
# .read_issued
# .read_sectors
# .write_issued
# .write_sectors
# For partitions, these *_issued are counters collected before
# requests are merged, so aren't the same as *_requests (which is
# post-merge, which more closely represents represents the actual
# number of disk transactions).

# Given that diskstats provides both per-disk and per-partition data,
# for TSDB purposes we want to put them under different metrics (versus
# the same metric and different tags).  Otherwise, if you look at a
# given metric, the data for a given box will be double-counted, since
# a given operation will increment both the disk series and the
# partition series.  To fix this, we output by-disk data to iostat.disk.*
# and by-partition data to iostat.part.*.

# TODO: Add additional tags to map partitions/disks back to mount
# points/swap so you can (for example) plot just swap partition
# activity or /var/lib/mysql partition activity no matter which
# disk/partition this happens to be.  This is nontrivial, especially
# when you have to handle mapping of /dev/mapper to dm-N, pulling out
# swap partitions from /proc/swaps, etc.

from __future__ import print_function

import copy
import os
import re
import sys
import time

from collectors.lib import utils

PY3 = sys.version_info[0] > 2
COLLECTION_INTERVAL = 60  # seconds

# Docs come from the Linux kernel's Documentation/iostats.txt
FIELDS_DISK = (
  "read_requests",  # Total number of reads completed successfully.
  "read_merged",  # Adjacent read requests merged in a single req.
  "read_sectors",  # Total number of sectors read successfully.
  "msec_read",  # Total number of ms spent by all reads.
  "write_requests",  # total number of writes completed successfully.
  "write_merged",  # Adjacent write requests merged in a single req.
  "write_sectors",  # total number of sectors written successfully.
  "msec_write",  # Total number of ms spent by all writes.
  "ios_in_progress",  # Number of actual I/O requests currently in flight.
  "msec_total",  # Amount of time during which ios_in_progress >= 1.
  "msec_weighted_total",  # Measure of recent I/O completion time and backlog.
)

FIELDS_PART = (
  "read_issued",
  "read_sectors",
  "write_issued",
  "write_sectors",
)

prev_times = (0, 0)


def read_uptime():
  global prev_times
  try:
    f_uptime = open("/proc/uptime")
    line = f_uptime.readline()

    curr_times = line.split(None)
    delta_times = (float(curr_times[0]) - float(prev_times[0]), float(curr_times[1]) - float(prev_times[1]))
    prev_times = curr_times
    return delta_times
  finally:
    f_uptime.close();


def get_system_hz():
  """Return system hz use SC_CLK_TCK."""
  ticks = os.sysconf(os.sysconf_names['SC_CLK_TCK'])

  if ticks == -1:
    return 100
  else:
    return ticks


def is_device(device_name, allow_virtual):
  """Test whether given name is a device or a partition, using sysfs."""
  device_name = re.sub('/', '!', device_name)

  if allow_virtual:
    devicename = "/sys/block/" + device_name + "/device"
  else:
    devicename = "/sys/block/" + device_name

  return os.access(devicename, os.F_OK)


cached_device_sector_size = {}


def get_device_sector_size(device_name):
  if device_name not in cached_device_sector_size:
    filename = "/sys/block/" + device_name + "sda/queue/hw_sector_size"

    if os.path.exists(filename):
      with open(filename, "r") as f:
        sector_size = int(f.readline())
    else:
      # hmmm?
      sector_size = 512;  # best guess
    cached_device_sector_size[device_name] = sector_size
  return cached_device_sector_size[device_name]


def main():
  """iostats main loop."""
  init_stats = {
    "read_requests": 0,
    "read_merged": 0,
    "read_sectors": 0,
    "msec_read": 0,
    "write_requests": 0,
    "write_merged": 0,
    "write_sectors": 0,
    "msec_write": 0,
    "ios_in_progress": 0,
    "msec_total": 0,
    "msec_weighted_total": 0,
  }
  prev_stats = dict()
  f_diskstats = open("/proc/diskstats")
  HZ = get_system_hz()
  itv = 1.0
  utils.drop_privileges()

  while True:
    f_diskstats.seek(0)
    ts = int(time.time())
    itv = read_uptime()[0]
    for line in f_diskstats:
      # maj, min, devicename, [list of stats, see above]
      values = line.split(None)
      # shortcut the deduper and just skip disks that
      # haven't done a single read.  This eliminates a bunch
      # of loopback, ramdisk, and cdrom devices but still
      # lets us report on the rare case that we actually use
      # a ramdisk.
      if values[3] == "0":
        continue

      if int(values[1]) % 16 == 0 and int(values[0]) > 1:
        metric = "iostat.disk."
      else:
        metric = "iostat.part."

      device = values[2]
      if len(values) == 14:
        # full stats line
        for i in range(11):
          print("%s%s %d %s dev=%s"
                % (metric, FIELDS_DISK[i], ts, values[i + 3], device))
          if FIELDS_DISK[i] == "read_sectors":
            if PY3:
              v = int(values[i + 3]) * get_device_sector_size(device)
            else:
              # noinspection PyCompatibility
              v = long(values[i + 3]) * get_device_sector_size(device)  # pylint:disable=undefined-variable
            print("%s%s %d %s dev=%s"
                  % (metric, "read_bytes", ts, v, device))
          if FIELDS_DISK[i] == "write_sectors":
            if PY3:
              v = int(values[i + 3]) * get_device_sector_size(device)
            else:
              # noinspection PyCompatibility
              v = long(values[i + 3]) * get_device_sector_size(device)  # pylint:disable=undefined-variable
            print("%s%s %d %s dev=%s"
                  % (metric, "write_bytes", ts, v, device))

        ret = is_device(device, 0)
        # if a device or a partition, calculate the svctm/await/util
        if ret:
          stats = dict(zip(FIELDS_DISK, values[3:]))
          if not device in prev_stats:
            prev_stats[device] = init_stats
          rd_ios = float(stats.get("read_requests"))
          wr_ios = float(stats.get("write_requests"))
          nr_ios = rd_ios + wr_ios
          prev_rd_ios = float(prev_stats[device].get("read_requests"))
          prev_wr_ios = float(prev_stats[device].get("write_requests"))
          prev_nr_ios = prev_rd_ios + prev_wr_ios
          tput = ((nr_ios - prev_nr_ios) * float(HZ) / float(itv))
          util = (
            (float(stats.get("msec_total")) - float(prev_stats[device].get("msec_total"))) * float(HZ) / float(itv))
          svctm = 0.0
          await_ = 0.0
          r_await = 0.0
          w_await = 0.0

          if tput:
            svctm = util / tput

          rd_ticks = stats.get("msec_read")
          wr_ticks = stats.get("msec_write")
          prev_rd_ticks = prev_stats[device].get("msec_read")
          prev_wr_ticks = prev_stats[device].get("msec_write")
          if rd_ios != prev_rd_ios:
            r_await = (float(rd_ticks) - float(prev_rd_ticks)) / float(rd_ios - prev_rd_ios)
          if wr_ios != prev_wr_ios:
            w_await = (float(wr_ticks) - float(prev_wr_ticks)) / float(wr_ios - prev_wr_ios)
          if nr_ios != prev_nr_ios:
            await_ = (float(rd_ticks) + float(wr_ticks) - float(prev_rd_ticks) - float(prev_wr_ticks)) / float(
              nr_ios - prev_nr_ios)
          print("%s%s %d %.2f dev=%s"
                % (metric, "svctm", ts, svctm, device))
          print("%s%s %d %.2f dev=%s"
                % (metric, "r_await", ts, r_await, device))
          print("%s%s %d %.2f dev=%s"
                % (metric, "w_await", ts, w_await, device))
          print("%s%s %d %.2f dev=%s"
                % (metric, "await", ts, await_, device))
          print("%s%s %d %.2f dev=%s"
                % (metric, "util", ts, float(util / 1000.0), device))

          prev_stats[device] = copy.deepcopy(stats)

      elif len(values) == 7:
        # partial stats line
        for i in range(4):
          print("%s%s %d %s dev=%s"
                % (metric, FIELDS_PART[i], ts, values[i + 3], device))
      else:
        print("Cannot parse /proc/diskstats line: ", line, file=sys.stderr)
        continue

    sys.stdout.flush()
    time.sleep(COLLECTION_INTERVAL)


if __name__ == "__main__":
  main()
