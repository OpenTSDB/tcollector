#!/usr/bin/env python
# Mysql stat collector.
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
"""mysql status stats for TSDB """


"""
Aborted_clients
Aborted_connects
Created_tmp_files
Created_tmp_disk_tables
Created_tmp_tables
Select_full_join
Threads_connected
Connections
Max_used_connections
Queries
Slow_queries
"""

import MySQLdb
import time
import sys

COLLECTION_INTERVAL = 10  # seconds

MYSQLSTATUS_EVALUATE = ['Aborted_clients',
                       'Aborted_connects',
                       'Created_tmp_files',
                       'Created_tmp_disk_tables',
                       'Created_tmp_tables',
                       'Select_full_join',
                       'Threads_connected',
                       'Max_used_connections',
                       'Connections',
                       'Queries',
                       'Slow_queries']

def main():

   while True:
       ts = int(time.time())

       try:
           db = MySQLdb.connect(host='127.0.0.1', user='root', passwd="root", db="gutefrage4")
         
           cursor = db.cursor(MySQLdb.cursors.DictCursor)

           cursor.execute("SHOW GLOBAL STATUS")
           statusvars = cursor.fetchall()

           for var in statusvars:
             if var['Variable_name'] in MYSQLSTATUS_EVALUATE:
               print ("mysql.%s %d %s" % (var['Variable_name'].lower(), ts,var["Value"]))
           cursor.close()
           db.close()
       except MySQLdb.Error, e:
         print >> sys.stderr, "MySQLdb error %d: %s" % (e.args[0],e.args[1])
         time.sleep(COLLECTION_INTERVAL*5)

       sys.stdout.flush()
       time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
   main()
