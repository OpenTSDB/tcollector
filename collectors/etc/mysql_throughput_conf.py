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

def enabled():
  return False

def get_config():
  config = {
    'sleep_time': 15,
    'metric_prefix': 'mysql.tcpdump',
    'tmp_dir': '/tmp', # temporary directory to persist work files
    'tcpdump_raw_file': 'tcpdump.out',  # filename to store raw tcpdump file to be generated
    'model_work_file': 'tcpdump.temp', # filename to store ASCII-ized output file from raw tcpdump data
    'result_file': 'sliced.txt' # filename to store cleaned up analysis data
  }
  return config
