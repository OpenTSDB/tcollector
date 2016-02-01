#!/usr/bin/env python
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
"""Send varnishstat counters to TSDB"""

# Please note, varnish 3.0.3 and above is necessary to run this script,
# earlier versions don't support json output.

import json
import subprocess
import sys
import time
import re

from collectors.lib import utils

interval = 10 # seconds

# If you would rather use the timestamp returned by varnishstat instead of a
# local timestamp, then change this value to "True"
use_varnishstat_timestamp = False

# This prefix will be prepended to all metric names before being sent
metric_prefix = "varnish"

# Add any additional tags you would like to include into this array as strings
#
# tags = ["production=false", "cloud=amazon"]
tags = []

# Collect all metrics
vstats = "all"

# Collect metrics a la carte
# vstats = frozenset([
#   "client_conn",
#   "client_drop",
#   "client_req",
#   "cache_hit",
#   "cache_hitpass",
#   "cache_miss"
# ])

def main():
 utils.drop_privileges()
 bad_regex = re.compile("[,()]+")  # avoid forbidden by TSD symbols

 while True:
    try:
      if vstats == "all":
        stats = subprocess.Popen(
          ["varnishstat", "-1", "-j"],
          stdout=subprocess.PIPE,
        )
      else:
        fields = ",".join(vstats)
        stats = subprocess.Popen(
          ["varnishstat", "-1", "-f" + fields, "-j"],
          stdout=subprocess.PIPE,
        )
    except OSError, e:
      # Die and signal to tcollector not to run this script.
      sys.stderr.write("Error: %s\n" % e)
      sys.exit(13)

    metrics = ""
    for line in stats.stdout.readlines():
      metrics += line
    metrics = json.loads(metrics)

    timestamp = ""
    if use_varnishstat_timestamp:
      pattern = "%Y-%m-%dT%H:%M:%S"
      timestamp = int(time.mktime(time.strptime(metrics['timestamp'], pattern)))
    else:
      timestamp = time.time()

    for k, v in metrics.iteritems():
      if k != "timestamp" and None == bad_regex.search(k):
        metric_name = metric_prefix + "." + k
        print "%s %d %s %s" % \
          (metric_name, timestamp, v['value'], ",".join(tags))

    sys.stdout.flush()
    time.sleep(interval)

if __name__ == "__main__":
  sys.exit(main())
