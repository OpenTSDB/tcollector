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

def err(msg):
  print >>sys.stderr, msg

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
    err("Couldn't connect to DB :%s" % (e))

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

    result = {}
    for stat in stats:
      info = {}
      info["numbackends"] = stat[2]
      info["xact_commit"] = stat[3]
      info["xact_rollback"] = stat[4]
      info["blks_read"] = stat[5]
      info["blks_hit"] = stat[6]
      info["tup_returned"] = stat[7]
      info["tup_fetched"] = stat[8]
      info["tup_inserted"] = stat[9]
      info["tup_updated"] = stat[10]
      info["tup_deleted"] = stat[11]
      info["conflicts"] = stat[12]
      info["size"] = stat[14]

      database = stat[1]
      result[database] = info

    for database in result:
      for metric, value in info.iteritems():
        print ("postgresql.%s %i %s database=%s"
               % (metric, ts, value, database))

    # connections
    cursor.execute("SELECT datname, count(datname) FROM pg_stat_activity"
                   " GROUP BY pg_stat_activity.datname")
    ts = time.time()
    connections = cursor.fetchall()

    for database, connection in connections:
      print ("postgresql.connections %i %s database=%s"
             % (ts, connection, database))

    cursor.close()

  except (EnvironmentError, EOFError, RuntimeError, socket.error), e:
    if isinstance(e, IOError) and e[0] == errno.EPIPE:
      # exit on a broken pipe. There is no point in continuing
      # because no one will read our stdout anyway.
      return 2
    err("error: failed to collect data: %s" % e)

def main(args):
  """Collects and dumps stats from a PostgreSQL server."""

  if psycopg2 is None:
    err("error: Python module 'psycopg2' is missing")
    return 13 # Ask tcollector to not respawn us

  sockdir = find_sockdir()
  if not sockdir: # Nothing to monitor
    err("error: Can't find postgresql socket file")
    return 13 # Ask tcollector to not respawn us

  db = postgres_connect(sockdir)

  while True:
    collect(db)
    sys.stdout.flush()
    time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
  sys.stdin.close()
  sys.exit(main(sys.argv))
