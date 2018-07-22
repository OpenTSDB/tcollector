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
Collector for PostgreSQL replication statistics.

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
import re
import subprocess

COLLECTION_INTERVAL = 5 # seconds

from collectors.lib import utils
from collectors.lib import postgresqlutils

def collect(db):
  """
  Collects and prints replication statistics.
  """

  try:
    cursor = db.cursor()

    # Replication lag time (could be slave only or a master / slave combo)
    cursor.execute("SELECT "
                   "CASE WHEN pg_is_in_recovery() THEN (EXTRACT (EPOCH FROM now() - pg_last_xact_replay_timestamp()) * 1000)::BIGINT ELSE NULL END AS replication_lag_time, "
                   "pg_xlog_location_diff(pg_last_xlog_receive_location(), pg_last_xlog_replay_location()) AS replication_lag_bytes, "
                   "pg_is_in_recovery() AS in_recovery;")
    ts = time.time()
    stats = cursor.fetchall()

    if (stats[0][0] is not None):
      print("postgresql.replication.upstream.lag.time %i %s"
             % (ts, stats[0][0]))

    if (stats[0][1] is not None):
      print("postgresql.replication.upstream.lag.bytes %i %s"
             % (ts, stats[0][1]))

    print("postgresql.replication.recovering %i %i"
           % (ts, stats[0][2]))

    #  WAL receiver process running (could be slave only or master / slave combo)
    ps_out = subprocess.check_output(["/bin/ps", "aux"] , stderr=subprocess.STDOUT)
    ps_out = ps_out.split("\n")
    ts = time.time()

    wal_receiver_running = 0
    for l in ps_out:
      l = l.strip()
      if (re.match (".*wal\\sreceiver.*", l)):
        wal_receiver_running = 1;
        break

    print("postgresql.replication.walreceiver.running %i %s"
         % (ts, wal_receiver_running))

    # WAL sender process info (could be master only or master / slave combo)
    cursor.execute("SELECT client_addr, client_port, "
                   "pg_xlog_location_diff(sent_location, replay_location) AS lag_bytes "
                   "FROM pg_stat_replication;")
    ts = time.time()
    stats = cursor.fetchall()

    print("postgresql.replication.downstream.count %i %i"
           % (ts, len(stats)))

    for stat in stats:
      print("postgresql.replication.downstream.lag.bytes %i %i client_ip=%s client_port=%s"
           % (ts, stat[2], stat[0], stat[1]))

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
