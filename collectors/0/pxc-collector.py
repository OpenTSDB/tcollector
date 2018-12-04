#!/usr/bin/env python

# This file is part of tcollector.
# Copyright (C) 2010-2016  The tcollector Authors.
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

"""
TCollector for Percona XtraDB Clusters

It's possible to query every single 'wsrep'-Variable.
If you want to add more variables simply edit the pxcconf.py

ATTENTION: Only tested on Debian/Ubuntu systems.
"""

import MySQLdb as mysql # pylint: disable=import-error
import time
import sys
import os.path
from collectors.etc import pxcconf
from collectors.lib import utils

__author__     = "Kai Laufer"
__version__    = "1.0.1"
__email__      = "mail@kai-laufer.de"

""" You can find these functions and additional information in etc/pxcconf.py """
prefix      = pxcconf.getPrefix() or "pxc" # Prefix for the collector, e.g. pxc -> pxc.wsrep_replicated_bytes
interval    = pxcconf.getInterval() or 1 # Interval for checking MySQL statistics
galeraFile  = pxcconf.getGaleraFile() or "/usr/lib/libgalera_smm.so" # Path to a galera specific file for ensuring that check won't run with a usual MySQL server. Default: "/usr/lib/libgalera_smm.so"
login       = pxcconf.getUserPassword() # MySQL-User, MySQL-Password and MySQL-Host (localhost)
myMap       = pxcconf.getKeyMap() or ( "wsrep_last_committed", "wsrep_replicated", "wsrep_repl_keys", "wsrep_local_commits" ) # Status variables which should be read
mysqlUser   = login[0] or "root"
mysqlPasswd = login[1] or ""
mysqlHost   = login[2] or "localhost"

def getRow():
        """ Test connection """
        try:
                db      = mysql.connect(host=mysqlHost, user=mysqlUser, passwd=mysqlPasswd)
                cursor  = db.cursor()
                cursor.execute("SHOW STATUS LIKE '%wsrep%'")
                result  = cursor.fetchall()

        except:
                print("Error: unable to fetch data - Check your configuration!")
                sys.exit(13) # Don't respawn collector

        db.close()
        return result

class TSDResult(object):
        """ Create TSD output """
        def __init__(self, key, value, prefix, timestamp):
                self.key       = key
                self.value     = value
                self.prefix    = prefix
                self.timestamp = timestamp

        def TSDRow(self):
                return "%s.%s %s %s" % (self.prefix, self.key, self.timestamp, self.value)

def main():
        if os.path.isfile(galeraFile) is True:
                while True:
                        rows = getRow()
                        for row in rows:
                                timestamp = int(time.time())
                                if row[0] in myMap:
                                        result = TSDResult(row[0], row[1], prefix, timestamp)
                                        print(result.TSDRow())
                        time.sleep(interval)
                return 0
        else:
                return 2

if __name__ == "__main__":
        sys.exit(main())
