#!/usr/bin/python
# HaProxy stat collector.
# Copyright (C) 2011  Gutefrage.net GmbH.
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
"""haproxy stats for TSDB """

#
# haproxy.py
#

import socket
import sys
import time
import urllib
import csv
import re

COLLECTION_INTERVAL = 10  # seconds

#HAPROXY_CSV_STAT_URL = 'file:///Users/fmk/Desktop/stats.csv'
HAPROXY_CSV_STAT_URL = 'http://admin:GF123NET@62.146.7.79/admin/stats?;csv'

def main():
    """haproxy main loop"""
    csvfile = None

    while True:
        ts = int(time.time())

        try:
            csvfile = urllib.urlopen(HAPROXY_CSV_STAT_URL);

            reader = csv.reader(csvfile, delimiter=',', quoting=csv.QUOTE_NONE)
            for row in reader:
                if row[0] == 'app' and row[1] == 'FRONTEND':
                    print ("haproxy.requests.total %d %s service=app" % (ts, row[48]))

                    print ("haproxy.requests %d %s code=5xx service=app" % (ts, row[43]))
                    print ("haproxy.requests %d %s code=4xx service=app" % (ts, row[42]))
                    print ("haproxy.requests %d %s code=3xx service=app" % (ts, row[41]))
                    print ("haproxy.requests %d %s code=2xx service=app" % (ts, row[40]))
                    print ("haproxy.requests %d %s code=1xx service=app" % (ts, row[39]))


                    print ("haproxy.session.total %d %s service=app" % (ts, row[7]))
                    print ("haproxy.session.current %d %s service=app" % (ts, row[4]))

                    print ("haproxy.session.max %d %s service=app" % (ts, row[5]))
                
                '''    
                if row[0] == 'dynamic' and row[1] == 'BACKEND':
                    print ("haproxy.queue.current %d %s cluster=app" % (ts, row[2]))
                    print ("haproxy.queue.max %d %s cluster=app" % (ts, row[3]))
                '''

            csvfile.close()

        except socket.error, (errno, msg):
            print >> sys.stderr, "haproxy returned %r" % msg
            if csvfile is not None:
                csvfile.close()
                csvfile = None;

        sys.stdout.flush()
        time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
    main()