#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2013 The tcollector Authors.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
# General Public License for more details. You should have received a copy
# of the GNU Lesser General Public License along with this program. If not,
# see <http://www.gnu.org/licenses/>.
"""
Collector for PostgreSQL.

Ensure that etc/postgresqlconf.py exists as defined at
https://github.com/OpenTSDB/tcollector/blob/v1.3.0/collectors/etc/postgresqlconf.py.

Note that the password may be left blank if HBA trusts local connections.
As this collector uses a socket file for its DB connection, ensure that 'unix_socket_directory'
is set in postgresql.conf.
"""

import sys
import os
import time
import socket
import errno

COLLECTION_INTERVAL = 15 # seconds

from collectors.lib import utils
from collectors.lib import postgresqlutils

def collect(db):
  """
  Collects and prints stats.

  Here we collect only general info, for full list of data for collection
  see http://www.postgresql.org/docs/9.2/static/monitoring-stats.html
  """

  try:
    cursor = db.cursor()

    # general statics
    cursor.execute("SELECT pg_stat_database.*, pg_database_size"
                   " (pg_database.datname) AS size FROM pg_database JOIN"
                   " pg_stat_database ON pg_database.datname ="
                   " pg_stat_database.datname WHERE pg_stat_database.datname"
                   " NOT IN ('template0', 'template1', 'postgres')")
    ts = time.time()
    stats = cursor.fetchall()

#  datid |  datname   | numbackends | xact_commit | xact_rollback | blks_read  |  blks_hit   | tup_returned | tup_fetched | tup_inserted | tup_updated | tup_deleted | conflicts | temp_files |  temp_bytes  | deadlocks | blk_read_time | blk_write_time |          stats_reset          |     size
    result = {}
    for stat in stats:
      database = stat[1]
      result[database] = stat

    for database in result:
      for i in range(2,len(cursor.description)):
        metric = cursor.description[i].name
        value = result[database][i]
        try:
          if metric in ("stats_reset"):
            continue
          print("postgresql.%s %i %s database=%s"
                 % (metric, ts, value, database))
        except:
          utils.err("got here")
          continue

    # connections
    cursor.execute("SELECT datname, count(datname) FROM pg_stat_activity"
                   " GROUP BY pg_stat_activity.datname")
    ts = time.time()
    connections = cursor.fetchall()

    for database, connection in connections:
      print("postgresql.connections %i %s database=%s"
             % (ts, connection, database))

  except (EnvironmentError, EOFError, RuntimeError, socket.error) as e:
    if isinstance(e, IOError) and e[0] == errno.EPIPE:
      # exit on a broken pipe. There is no point in continuing
      # because no one will read our stdout anyway.
      return 2
    utils.err("error: failed to collect data: %s" % e)

def main(args):
  """Collects and dumps stats from a PostgreSQL server."""

  try:
    db = postgresqlutils.connect()
  except (Exception) as e:
    utils.err("error: Could not initialize collector : %s" % (e))
    return 13 # Ask tcollector to not respawn us

  while True:
    collect(db)
    sys.stdout.flush()
    time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
  sys.stdin.close()
  sys.exit(main(sys.argv))