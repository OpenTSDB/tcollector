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
Collector Utilities for PostgreSQL.
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
        if (utils.is_sockfile(os.path.join(dirpath, name)) and "PGSQL" in name):
          return(dirpath)

def postgres_connect(sockdir):
  """Connects to the PostgreSQL server using the specified socket file."""
  user, password = postgresqlconf.get_user_password()

  try:
    return psycopg2.connect("host='%s' user='%s' password='%s' "
                            "connect_timeout='%s' dbname=postgres"
                            % (sockdir, user, password,
                            CONNECT_TIMEOUT))
  except (EnvironmentError, EOFError, RuntimeError, socket.error) as e:
    utils.err("Couldn't connect to DB :%s" % (e))

def connect():
  """Returns an initialized connection to Postgresql."""
  if psycopg2 is None:
    raise RuntimeError("error: Python module 'psycopg2' is missing")

  sockdir = find_sockdir()
  if not sockdir: # Nothing to monitor
    raise RuntimeError("error: Can't find postgresql socket file")

  db = postgres_connect(sockdir)
  db.autocommit=True

  return db