#!/usr/bin/env python

"""
TCollector for Percona XtraDB Clusters

It's possible to query every single 'wsrep'-Variable.
If you want to add more variables simply edit the pxcconf.py

ATTENTION: Only tested on Debian/Ubuntu systems.
"""

import MySQLdb as mysql
import time
import sys
import os.path
from collectors.etc import pxcconf
from collectors.lib import utils

__author__     = "Kai Laufer"
__license__    = "GPL"
__version__    = "1.0.0"
__email__      = "mail@kai-laufer.de"

""" You can find these functions in etc/pxcconf.py """
prefix      = pxcconf.getPrefix()
interval    = pxcconf.getInterval()
galeraFile  = pxcconf.getGaleraFile()
login       = pxcconf.getUserPassword()
myMap       = pxcconf.getKeyMap()
mysqlUser   = login[0]
mysqlPasswd = login[1]
mysqlHost   = login[2]

def getRow():
	db      = mysql.connect(host=mysqlHost, user=mysqlUser, passwd=mysqlPasswd)
	cursor  = db.cursor()
	cursor.execute("SHOW STATUS LIKE '%wsrep%'")
	result  = cursor.fetchall()
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
					print result.TSDRow()
			time.sleep(interval)
		return 0
	else:
		return 2

if __name__ == "__main__":
	sys.exit(main())
