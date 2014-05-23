#!/usr/bin/python
# This file is part of tcollector.
# Copyright (C) 2012  The tcollector Authors.
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

import errno
import sys
import time
import subprocess
import re
import signal
import os


'''
ZFS I/O and disk space statistics for TSDB

This plugin tracks, for all pools:

- I/O
  zfs.io.pool.{read_issued, write_issued}
  zfs.io.pool.{read_sectors, write_sectors}
  zfs.io.device.{read_issued, write_issued}
  zfs.io.device.{read_sectors, write_sectors}
- disk space
  zfs.df.pool.1kblocks.{total, used, available}
  zfs.df.device.1kblocks.{total, used, available}

Sectors are always 512 bytes.  Disk space usage is given in 1K blocks.
Values delivered to standard output are already normalized to be per second.
'''

def convert_to_bytes(string):
    """Take a string in the form 1234K, and convert to bytes"""
    factors = {
       "K": 1024,
       "M": 1024 * 1024,
       "G": 1024 * 1024 * 1024,
       "T": 1024 * 1024 * 1024 * 1024,
       "P": 1024 * 1024 * 1024 * 1024 * 1024,
    }
    if string == "-": return 0
    for f, fm in factors.items():
        if string.endswith(f):
            number = float(string[:-1])
            number = number * fm
            return long(number)
    return long(string)

def extract_info(line):
    (poolname,
        alloc, free,
        read_issued, write_issued,
        read_sectors, write_sectors) = line.split()

    s_df = {}
    # 1k blocks
    s_df["used"] = convert_to_bytes(alloc) / 1024
    s_df["available"] = convert_to_bytes(free) / 1024
    s_df["total"] = s_df["used"] + s_df["available"]

    s_io = {}
    # magnitudeless variable
    s_io["read_issued"] = read_issued
    s_io["write_issued"] = write_issued
    # 512 byte sectors
    s_io["read_sectors"] = convert_to_bytes(read_sectors) / 512
    s_io["write_sectors"] = convert_to_bytes(write_sectors) / 512

    return poolname, s_df, s_io

T_START = 1
T_HEADERS = 2
T_SEPARATOR = 3
T_POOL = 4
T_DEVICE = 5
T_EMPTY = 6
T_LEG = 7

signal_received = None
def handlesignal(signum, stack):
    global signal_received
    signal_received = signum

def main():
    """zfsiostats main loop"""
    global signal_received
    interval = 15
    # shouldn't the interval be determined by the daemon itself, and commu-
    # nicated to the collector somehow (signals seem like a reasonable protocol
    # whereas command-line parameters also sound reasonable)?

    signal.signal(signal.SIGTERM, handlesignal)
    signal.signal(signal.SIGINT, handlesignal)

    try:
        p_zpool = subprocess.Popen(
            ["zpool", "iostat", "-v", str(interval)],
            stdout=subprocess.PIPE,
        )
    except OSError, e:
        if e.errno == errno.ENOENT:
            # it makes no sense to run this collector here
            sys.exit(13) # we signal tcollector to not run us
        raise

    firstloop = True
    lastleg = 0
    ltype = None
    timestamp = int(time.time())
    capacity_stats_pool = {}
    capacity_stats_device = {}
    io_stats_pool = {}
    io_stats_device = {}
    start_re = re.compile(".*capacity.*operations.*bandwidth")
    headers_re = re.compile(".*pool.*alloc.*free.*read.*write.*read.*write")
    separator_re = re.compile(".*-----.*-----.*-----")
    while signal_received is None:
        try:
            line = p_zpool.stdout.readline()
        except (IOError, OSError), e:
            if e.errno in (errno.EINTR, errno.EAGAIN):
                break
            raise

        if not line:
            # end of the program, die
            break

        if start_re.match(line):
            assert ltype in (None, T_EMPTY), \
                "expecting last state T_EMPTY or None, now got %s" % ltype
            ltype = T_START
        elif headers_re.match(line):
            assert ltype == T_START, \
                "expecting last state T_START, now got %s" % ltype
            ltype = T_HEADERS
        elif separator_re.match(line):
            assert ltype in (T_DEVICE, T_HEADERS), \
                "expecting last state T_DEVICE or T_HEADERS, now got %s" % ltype
            ltype = T_SEPARATOR
        elif len(line) < 2:
            assert ltype == T_SEPARATOR, \
                "expecting last state T_SEPARATOR, now got %s" % ltype
            ltype = T_EMPTY
        elif line.startswith("  mirror"):
            assert ltype in (T_POOL, T_DEVICE), \
                "expecting last state T_POOL or T_DEVICE, now got %s" % ltype
            ltype = T_LEG
        elif line.startswith("  "):
            assert ltype in (T_POOL, T_DEVICE, T_LEG), \
                "expecting last state T_POOL or T_DEVICE or T_LEG, now got %s" % ltype
            ltype = T_DEVICE
        else:
            # must be a pool name
            assert ltype == T_SEPARATOR, \
                "expecting last state T_SEPARATOR, now got %s" % ltype
            ltype = T_POOL

        if ltype == T_START:
            for x in (
                      capacity_stats_pool, capacity_stats_device,
                      io_stats_pool, io_stats_device,
                      ):
                x.clear()
            timestamp = int(time.time())

        elif ltype == T_POOL:
            line = line.strip()
            poolname, s_df, s_io = extract_info(line)
            capacity_stats_pool[poolname] = s_df
            io_stats_pool[poolname] = s_io
            # marker for leg
            last_leg = 0

        elif ltype == T_LEG:
            last_leg = last_leg + 1
            line = line.strip()
            devicename, s_df, s_io = extract_info(line)
            capacity_stats_device["%s %s%s" % (poolname, devicename, last_leg)] = s_df
            io_stats_device["%s %s%s" % (poolname, devicename, last_leg)] = s_io

        elif ltype == T_DEVICE:
            line = line.strip()
            devicename, s_df, s_io = extract_info(line)
            capacity_stats_device["%s %s" % (poolname, devicename)] = s_df
            io_stats_device["%s %s" % (poolname, devicename)] = s_io

        elif ltype == T_EMPTY:
            if firstloop:
                firstloop = False
            else:
                # this flag prevents printing out of the data in the first loop
                # which is a since-boot summary similar to iostat
                # and is useless to us
                for poolname, stats in capacity_stats_pool.items():
                    fm = "zfs.df.pool.1kblocks.%s %d %s poolname=%s"
                    for statname, statnumber in stats.items():
                        print fm % (statname, timestamp, statnumber, poolname)
                for poolname, stats in io_stats_pool.items():
                    fm = "zfs.io.pool.%s %d %s poolname=%s"
                    for statname, statnumber in stats.items():
                        print fm % (statname, timestamp, statnumber, poolname)
                for devicename, stats in capacity_stats_device.items():
                    fm = "zfs.df.device.1kblocks.%s %d %s devicename=%s poolname=%s"
                    poolname, devicename = devicename.split(" ", 1)
                    for statname, statnumber in stats.items():
                        print fm % (statname, timestamp, statnumber,
                                    devicename, poolname)
                for devicename, stats in io_stats_device.items():
                    fm = "zfs.io.device.%s %d %s devicename=%s poolname=%s"
                    poolname, devicename = devicename.split(" ", 1)
                    for statname, statnumber in stats.items():
                        print fm % (statname, timestamp, statnumber,
                                    devicename, poolname)
                sys.stdout.flush()
                # if this was the first loop, well, we're onto the second loop
                # so we turh the flag off

    if signal_received is None:
        signal_received = signal.SIGTERM
    try:
        os.kill(p_zpool.pid, signal_received)
    except Exception:
        pass
    p_zpool.wait()

if __name__ == "__main__":
    main()

