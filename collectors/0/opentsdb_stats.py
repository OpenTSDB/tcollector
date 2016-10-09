#!/usr/bin/python
# This file is part of tcollector.
# Copyright (C) 2012  StumbleUpon, Inc.
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
"""Collect stats from OpenTSDB through /stats HTTP"""

import requests
import sys
import time

# TSD_STATS_URL = "http://127.0.0.1:4242/stats"
COLLECTION_INTERVAL = 10 * 60 # 10 minutes

def main():
    """opentsdb_stats main loop"""

    while True:
        response = requests.get(TSD_STATS_URL)
        if response.status_code == requests.codes.ok:
            print(response.text)
        else:
            print >> sys.stderr, "Request failed with status code %s." % response.status_code

        sys.stdout.flush()
        time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
    main()
