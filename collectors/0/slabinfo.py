#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2010-2013  The tcollector Authors.
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
"""memory slab info fragmentation counters for TSDB"""

import sys
import time
import socket

from collectors.lib import utils
from collectors.etc import slabinfo_conf as config

SLABINFO = '/proc/slabinfo'
METRIC = 'proc.meminfo.slabinfo'
HOSTNAME = socket.gethostname()
CONFIG = config.get_config()
COLLECTION_INTERVAL = CONFIG['interval']

if not CONFIG['enabled']:
    sys.stderr.write("Slab info collector is not enabled")
    sys.exit(13)

def main():
    """slabinfo collector main loop"""

    try:
        with open(SLABINFO,'r') as slabinfo:
            utils.drop_privileges()
            while True:
                #slabinfo.seek(204)
                slabinfo.seek(0)
                epoch = int(time.time())
                for line in slabinfo.readlines(): 
                    if 'slabinfo - version' in line or '#name' in line:
                        continue

                    slabdata = line.strip().split()
                    objname, active_objs, num_objs, objsize, objperslab, pagesperslab = slabdata[0:6]
                    print("%s.active %s %s host=%s slab=%s objsize=%s" % 
                            (METRIC, epoch, active_objs, HOSTNAME, objname, objsize))
                    print("%s.numobjs %s %s host=%s slab=%s objsize=%s" % 
                            (METRIC, epoch, num_objs, HOSTNAME, objname, objsize))

                sys.stdout.flush()
                time.sleep(COLLECTION_INTERVAL)

    except IOError, e:
        utils.err("error: can't open %s: %s" % (SLABINFO,e))
        return 13 # Ask tcollector to not respawn us

if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main())

