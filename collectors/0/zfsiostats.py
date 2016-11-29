#!/usr/bin/env python
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

'''
ZFS I/O and disk space statistics for TSDB

This plugin tracks, for all pools:

- I/O
  zfs.io.pool.{ops.read, ops.write}
  zfs.io.pool.{bps.read, bps.write}
  zfs.io.device.{ops.read, ops.write}
  zfs.io.device.{bps.read, bps.write}
- disk space
  zfs.df.pool.kb.{used, free}
  zfs.df.device.kb.{used, free}

Disk space usage is given in kbytes.
Throughput is given in operations/s and bytes/s.
'''

import errno
import sys
import time
import subprocess
import re
import signal
import os

from collectors.lib import utils

try:
    from collectors.etc import zfsiostats_conf
except ImportError:
    zfsiostats_conf = None

DEFAULT_COLLECTION_INTERVAL=15
DEFAULT_REPORT_CAPACITY_EVERY_X_TIMES=20
DEFAULT_REPORT_DISKS_IN_VDEVS=False

def convert_to_bytes(string):
    """Take a string in the form 1234K, and convert to bytes"""
    factors = {
       "K": 1024,
       "M": 1024 * 1024,
       "G": 1024 * 1024 * 1024,
       "T": 1024 * 1024 * 1024 * 1024,
       "P": 1024 * 1024 * 1024 * 1024 * 1024,
       "E": 1024 * 1024 * 1024 * 1024 * 1024 * 1024,
    }
    if string == "-": return -1
    for f, fm in factors.items():
        if string.endswith(f):
            number = float(string[:-1])
            number = number * fm
            return long(number)
    return long(string)

def convert_wo_prefix(string):
    """Take a string in the form 1234K, and convert without metric prefix"""
    factors = {
       "K": 1000,
       "M": 1000 * 1000,
       "G": 1000 * 1000 * 1000,
       "T": 1000 * 1000 * 1000 * 1000,
       "P": 1000 * 1000 * 1000 * 1000 * 1000,
       "E": 1000 * 1000 * 1000 * 1000 * 1000 * 1000,
    }
    if string == "-": return -1
    for f, fm in factors.items():
        if string.endswith(f):
            number = float(string[:-1])
            number = number * fm
            return long(number)
    return long(string)

def extract_info(line,report_disks_in_vdevs):
    (poolname,
        alloc, free,
        read_issued, write_issued,
        read_throughput, write_throughput) = line.split()

    s_io = {}
    # magnitudeless variable
    s_io["ops.read"] = convert_wo_prefix(read_issued)
    s_io["ops.write"] = convert_wo_prefix(write_issued)
    # throughput
    s_io["bps.read"] = convert_to_bytes(read_throughput)
    s_io["bps.write"] = convert_to_bytes(write_throughput)
    if ((s_io["ops.read"] < 0) or (s_io["ops.write"] < 0) or (s_io["bps.read"] < 0) or (s_io["bps.write"] < 0)):
        s_io = {}

    s_df = {}
    # 1k blocks
    s_df["used"] = convert_to_bytes(alloc) / 1024
    s_df["free"] = convert_to_bytes(free) / 1024
    if ((s_df["used"] < 0) or (s_df["free"] < 0)):
        s_df = {}
        if(not report_disks_in_vdevs):
            s_io = {}

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

    collection_interval=DEFAULT_COLLECTION_INTERVAL
    report_capacity_every_x_times=DEFAULT_REPORT_CAPACITY_EVERY_X_TIMES
    report_disks_in_vdevs=DEFAULT_REPORT_DISKS_IN_VDEVS
    if(zfsiostats_conf):
        config = zfsiostats_conf.get_config()
        collection_interval=config['collection_interval']
        report_capacity_every_x_times=config['report_capacity_every_x_times']
        report_disks_in_vdevs=config['report_disks_in_vdevs']

    signal.signal(signal.SIGTERM, handlesignal)
    signal.signal(signal.SIGINT, handlesignal)

    try:
        p_zpool = subprocess.Popen(
            ["zpool", "iostat", "-v", str(collection_interval)],
            stdout=subprocess.PIPE,
        )
    except OSError, e:
        if e.errno == errno.ENOENT:
            # it makes no sense to run this collector here
            sys.exit(13) # we signal tcollector to not run us
        raise

    firstloop = True
    report_capacity = (report_capacity_every_x_times-1)
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
            #assert ltype == T_SEPARATOR, \
            #    "expecting last state T_SEPARATOR, now got %s" % ltype
            if ltype == T_SEPARATOR:
                parentpoolname = ""
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
            poolname, s_df, s_io = extract_info(line,report_disks_in_vdevs)
            if parentpoolname == "":
                parentpoolname = poolname
            else:
                poolname=parentpoolname+"."+poolname
            capacity_stats_pool[poolname] = s_df
            io_stats_pool[poolname] = s_io
            # marker for leg
            last_leg = 0

        elif ltype == T_LEG:
            last_leg = last_leg + 1
            line = line.strip()
            devicename, s_df, s_io = extract_info(line,report_disks_in_vdevs)
            capacity_stats_device["%s %s%s" % (poolname, devicename, last_leg)] = s_df
            io_stats_device["%s %s%s" % (poolname, devicename, last_leg)] = s_io

        elif ltype == T_DEVICE:
            line = line.strip()
            devicename, s_df, s_io = extract_info(line,report_disks_in_vdevs)
            capacity_stats_device["%s %s" % (poolname, devicename)] = s_df
            io_stats_device["%s %s" % (poolname, devicename)] = s_io

        elif ltype == T_EMPTY:
            if report_capacity_every_x_times > 0:
                report_capacity += 1
            if report_capacity == report_capacity_every_x_times:
                report_capacity=0
                for poolname, stats in capacity_stats_pool.items():
                    fm = "zfs.df.pool.kb.%s %d %s pool=%s"
                    for statname, statnumber in stats.items():
                        print fm % (statname, timestamp, statnumber, poolname)
                for devicename, stats in capacity_stats_device.items():
                    fm = "zfs.df.device.kb.%s %d %s device=%s pool=%s"
                    poolname, devicename = devicename.split(" ", 1)
                    for statname, statnumber in stats.items():
                        print fm % (statname, timestamp, statnumber,
                                    devicename, poolname)
            if firstloop:
                # this flag prevents printing out of the data in the first loop
                # which is a since-boot summary similar to iostat
                # and is useless to us
                firstloop = False
            else:
                for poolname, stats in io_stats_pool.items():
                    fm = "zfs.io.pool.%s %d %s pool=%s"
                    for statname, statnumber in stats.items():
                        print fm % (statname, timestamp, statnumber, poolname)
                for devicename, stats in io_stats_device.items():
                    fm = "zfs.io.device.%s %d %s device=%s pool=%s"
                    poolname, devicename = devicename.split(" ", 1)
                    for statname, statnumber in stats.items():
                        print fm % (statname, timestamp, statnumber,
                                    devicename, poolname)
            sys.stdout.flush()

    if signal_received is None:
        signal_received = signal.SIGTERM
    try:
        os.kill(p_zpool.pid, signal_received)
    except Exception:
        pass
    p_zpool.wait()

if __name__ == "__main__":
    main()

