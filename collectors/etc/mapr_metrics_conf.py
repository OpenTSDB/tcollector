#!/usr/bin/python
# This file is part of tcollector.
# Copyright (C) 2015  The tcollector Authors.
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
# Before you enable the mapr_metrics collector, create the metrics:
# tsdb mkmetric mapr.disks.READBYTES mapr.disks.WRITEOPS mapr.disks.WRITEBYTES mapr.disks.READOPS mapr.cpus.CPUTOTAL mapr.cpus.CPUIDLE mapr.cpus.CPUIOWAIT mapr.network.PKTSOUT mapr.network.BYTESOUT mapr.network.PKTSIN mapr.network.BYTESIN mapr.memory.used mapr.mfs.available mapr.mfs.used
#

def enabled():
  return False

def get_config():
  config = {
    'interval': 15,
    'webserver': 'localhost',
    'port': 8443,
    'no_ssl': False,
    'username': 'metrics',
    'password': 'maprmetrics'
  }
  return config
