#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2016  The tcollector Authors.
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
"""netfilter stats for TSDB. This collector exposes metrics from /proc/sys/net/ipv4/netfilter/*. 
   Note that the plugin also collects the setting values from this directory, as it makes
   it possible to monitor for incorrect settings, and also gives access to the value of 
   these for non-root users."""

import sys
import time
import os

from collectors.lib import utils

interval = 15  # seconds

STATS = ("nf_conntrack_buckets", "nf_conntrack_checksum", "nf_conntrack_count",
         "nf_conntrack_generic_timeout", "nf_conntrack_icmp_timeout",
         "nf_conntrack_log_invalid", "nf_conntrack_max", "nf_conntrack_tcp_be_liberal",
         "nf_conntrack_tcp_loose", "nf_conntrack_tcp_max_retrans",
         "nf_conntrack_tcp_timeout_close", "nf_conntrack_tcp_timeout_close_wait",
         "nf_conntrack_tcp_timeout_established", "nf_conntrack_tcp_timeout_fin_wait",
         "nf_conntrack_tcp_timeout_last_ack", "nf_conntrack_tcp_timeout_max_retrans",
         "nf_conntrack_tcp_timeout_syn_recv", "nf_conntrack_tcp_timeout_syn_sent",
         "nf_conntrack_tcp_timeout_time_wait", "nf_conntrack_udp_timeout",
         "nf_conntrack_udp_timeout_stream")

basedir = "/proc/sys/net/netfilter"

def main():
    """netfilter main loop"""

    utils.drop_privileges()

    if (os.path.isdir(basedir)): 
        while True:
            ts = int(time.time())
        
            for s in STATS: 
                try: 
                   f = open(basedir + "/" + s, 'r')
                   value = f.readline().rstrip()
                   print("proc.sys.net.netfilter.%s %d %s" % (s, ts, value))
                   f.close() 
                except:
                   # brute'ish, but should keep the collector reasonably future 
                   # proof if some of the stats disappear between kernel module 
                   # versions
                   continue

            sys.stdout.flush()
            time.sleep(interval)
    else: 
        print ("%s does not exist - ip_conntrack probably missing")
        sys.exit(13) # we signal tcollector to not run us
        

if __name__ == "__main__":
    sys.exit(main())
