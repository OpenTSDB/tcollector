#!/usr/bin/env python
# NTP Offset stats
# charlesrg AT gmail.com
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
"""ntp offset stats for TSDB """
#
# ntpstat.py
#
# ntp.offset             estimated offset

from __future__ import print_function

import os
import socket
import subprocess
import sys
import time
import errno

from collectors.lib import utils

try:
  from collectors.etc import ntpstat_conf
except ImportError:
  ntpstat_conf = None

DEFAULT_COLLECTION_INTERVAL=60

def main():
    """ntpstats main loop"""

    collection_interval=DEFAULT_COLLECTION_INTERVAL
    if(ntpstat_conf):
        config = ntpstat_conf.get_config()
        collection_interval=config['collection_interval']

    utils.drop_privileges()

    while True:
        ts = int(time.time())
        try:
            ntp_proc = subprocess.Popen(["ntpq", "-p"], stdout=subprocess.PIPE)
        except OSError as e:
            if e.errno == errno.ENOENT:
                # looks like ntpdc is not available, stop using this collector
                sys.exit(13) # we signal tcollector to stop using this
            raise

        stdout, _ = ntp_proc.communicate()
        if ntp_proc.returncode == 0:
            for line in stdout.split("\n"):
                if not line:
                    continue
                fields = line.split()
                if len(fields) <= 0:
                    continue
                if fields[0].startswith("*"):
                    offset=fields[8]
                    continue
            print("ntp.offset %d %s" % (ts, offset))
        else:
            print("ntpq -p, returned %r" % (ntp_proc.returncode), file=sys.stderr)

        sys.stdout.flush()
        time.sleep(collection_interval)

if __name__ == "__main__":
    main()
