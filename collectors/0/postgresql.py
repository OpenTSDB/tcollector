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

Please, set login/password at etc/postgresql.conf .
Collector uses socket file for DB connection so set 'unix_socket_directory'
at postgresql.conf .
"""

import sys
import os
import time
import socket
import errno

try:
  import psycopg2
except ImportError:
  psycopg2 = None # handled in main()

COLLECTION_INTERVAL = 15 # seconds
CONNECT_TIMEOUT = 2 # seconds

from collectors.lib import utils
from collectors.etc import postgresqlconf

# Directories under which to search socket files
SEARCH_DIRS = frozenset([
  "/var/run/postgresql", # Debian default
  "/var/pgsql_socket", # MacOS default
  "/usr/local/var/postgres", # custom compilation
  "/tmp", # custom compilation
])

def find_sockdir():
  """Returns a path to PostgreSQL socket file to monitor."""
  for dir in SEARCH_DIRS:
    for dirpath, dirnames, dirfiles in os.walk(dir, followlinks=True):
      for name in dirfiles:
        # ensure selection of PostgreSQL socket only
	if (utils.is_sockfile(os.path.join(dirpath, name))
	    and "PGSQL" in name):
          return(dirpath)

def postgres_connect(sockdir):
  """Connects to the PostgreSQL server using the specified socket file."""
  user, password = postgresqlconf.get_user_password()

  try:
    return psycopg2.connect("host='%s' user='%s' password='%s' "
                            "connect_timeout='%s' dbname=postgres"
                            % (sockdir, user, password,
                            CONNECT_TIMEOUT))
  except (EnvironmentError, EOFError, RuntimeError, socket.error), e:
    utils.err("Couldn't connect to DB :%s" % (e))

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
          print ("postgresql.%s %i %s database=%s"
                 % (metric, ts, value, database))
        except:
          err("got here")
          continue

    # connections
    cursor.execute("SELECT datname, count(datname) FROM pg_stat_activity"
                   " GROUP BY pg_stat_activity.datname")
    ts = time.time()
    connections = cursor.fetchall()

    for database, connection in connections:
      print ("postgresql.connections %i %s database=%s"
             % (ts, connection, database))

  except (EnvironmentError, EOFError, RuntimeError, socket.error), e:
    if isinstance(e, IOError) and e[0] == errno.EPIPE:
      # exit on a broken pipe. There is no point in continuing
      # because no one will read our stdout anyway.
      return 2
    utils.err("error: failed to collect data: %s" % e)

def main(args):
  """Collects and dumps stats from a PostgreSQL server."""

  if psycopg2 is None:
    utils.err("error: Python module 'psycopg2' is missing")
    return 13 # Ask tcollector to not respawn us

  sockdir = find_sockdir()
  if not sockdir: # Nothing to monitor
    utils.err("error: Can't find postgresql socket file")
    return 13 # Ask tcollector to not respawn us

  db = postgres_connect(sockdir)
  db.autocommit=True

  while True:
    collect(db)
    sys.stdout.flush()
    time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
  sys.stdin.close()
  sys.exit(main(sys.argv))
