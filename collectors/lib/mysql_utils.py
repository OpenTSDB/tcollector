#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2011  The tcollector Authors.
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
"""Utils to write collectors for MySQL."""

import errno
import os
import re
import socket
import sys
import time
import yaml

import MySQLdb
import commands

from collectors.etc import mysqlconf
from collectors.lib import utils

MAX_DB_QUERY_RETRY = 3
CONNECT_TIMEOUT = 2  # seconds
# How frequently we try to find new databases.
DB_REFRESH_INTERVAL = 60  # seconds
# Usual locations where to find the default socket file.
DEFAULT_SOCKFILES = set([
    "/tmp/mysql.sock",                  # MySQL's own default.
    "/var/lib/mysql/mysql.sock",        # RH-type / RPM systems.
    "/var/run/mysqld/mysqld.sock",      # Debian-type systems.
])
# Directories under which to search additional socket files.
SEARCH_DIRS = [
    "/var/lib/mysql",
]


class DB(object):
    """Represents a MySQL server (as we can monitor more than 1 MySQL)."""

    def __init__(self, sockfile, dbname, db, cursor, version):
        """Constructor.

        Args:
            sockfile: Path to the socket file.
            dbname: Name of the database for that socket file.
            db: A MySQLdb connection opened to that socket file.
            cursor: A cursor acquired from that connection.
            version: What version is this MySQL running (from `SELECT VERSION()').
        """
        self.sockfile = sockfile
        self.dbname = dbname
        self.db = db
        self.cursor = cursor
        self.version = version
        self.master = None
        self.slave_bytes_executed = None
        self.relay_bytes_relayed = None
        self.is_master = False
        self.is_slave = False
        self.attached_slaves = []
        self.slave_status = {}
        self.is_dump_server = False

        try:
            with open('/etc/salt/grains') as f:
                grains = yaml.load(f)
                if 'dump_server' in grains:
                    self.is_dump_server = bool(grains['dump_server'])
        except:
            pass

        version = version.split(".")
        try:
            self.major = int(version[0])
            self.medium = int(version[1])
        except (ValueError, IndexError):
            self.major = self.medium = 0

        # initialize db master/slave status, which will be updated in each collect
        self.check_set_master()
        self.check_set_slave()

    def __str__(self):
        return "DB(%r, %r, version=%r)" % (self.sockfile, self.dbname,
                                           self.version)

    def __repr__(self):
        return self.__str__()

    def check_set_master(self):
        """If DB has slaves attached to it, set is_master to True.

        NOTE: A DB can be both master and slave, in a multi-tiered setup.
        """
        attached_slaves = self.query("SHOW SLAVE HOSTS")
        if attached_slaves:
            self.is_master = True
            self.attached_slaves = attached_slaves
        else:
            self.is_master = False
            self.attached_slaves = []

    def check_set_slave(self):
        """If DB attaches to a master, set is_slave to True.

        NOTE: A DB can be both master and slave, in a multi-tiered setup.
        """
        mysql_slave_status = self.query("SHOW SLAVE STATUS")
        if mysql_slave_status:
            self.is_slave = True
            self.slave_status = to_dict(self, mysql_slave_status[0])
        else:
            self.is_slave = False
            self.slave_status = {}

    def isShowGlobalStatusSafe(self):
        """Returns whether or not SHOW GLOBAL STATUS is safe to run."""
        # We can't run SHOW GLOBAL STATUS on versions prior to 5.1 because it
        # locks the entire database for too long and severely impacts traffic.
        return self.major > 5 or (self.major == 5 and self.medium >= 1)

    def query(self, sql):
        """Executes the given SQL statement and returns a sequence of rows."""
        assert self.cursor, "%s already closed?" % (self,)
        for _ in range(MAX_DB_QUERY_RETRY):
            try:
                self.cursor.execute(sql)
                return self.cursor.fetchall()
            except MySQLdb.OperationalError, (errcode, msg):
                if errcode != 2006:  # "MySQL server has gone away"
                    raise
                self._reconnect()
        return []

    def close(self):
        """Closes the connection to this MySQL server."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.db:
            self.db.close()
            self.db = None

    def _reconnect(self):
        """Reconnects to this MySQL server."""
        self.close()
        self.db = mysql_connect(self.sockfile)
        self.cursor = self.db.cursor()


def mysql_connect(sockfile):
    """Connects to the MySQL server using the specified socket file."""
    user, passwd = mysqlconf.get_user_password(sockfile)
    return MySQLdb.connect(unix_socket=sockfile,
                           connect_timeout=CONNECT_TIMEOUT,
                           user=user, passwd=passwd)


def to_dict(db, row):
    """Transforms a row (returned by DB.query) into a dict keyed by column names.

    Args:
        db: The DB instance from which this row was obtained.
        row: A row as returned by DB.query
    """
    d = {}
    for i, field in enumerate(db.cursor.description):
        column = field[0].lower()  # Lower-case to normalize field names.
        d[column] = row[i]
    return d


def get_houzz_db_name():
    """Houzz specific logic to parse out shard name from hostname."""
    hostname = socket.gethostname()
    if hostname.startswith('mysql-'):
        # ex.: mysql-master-kv-04681cb62eb0d3660.web-production.houzz.net
        m = re.match(r'mysql-(master|slave)-([^-]+)-.+', hostname.split('.')[0])
        if m:
            return m.group(2)
        else:
            # handle new host name for ubuntu upgrade
            m = re.match(r'mysql-([^-]+)-.+', hostname.split('.')[0])
            if m:
                return m.group(1)
            return "main"

    elif hostname.startswith('pay-'):
        return "payment"

    return "default"


def get_dbname(sockfile):
    """Returns the name of the DB based on the path to the socket file."""
    if sockfile in DEFAULT_SOCKFILES:
        return get_houzz_db_name()
    m = re.search("/mysql-(.+)/[^.]+\.sock$", sockfile)
    if not m:
        utils.err("error: couldn't guess the name of the DB for " + sockfile)
        return None
    return m.group(1)


def find_sockfiles():
    """Returns a list of paths to socket files to monitor."""
    paths = []
    # Look for socket files.
    for dir in SEARCH_DIRS:
        if not os.path.isdir(dir) or not os.access(dir, os.R_OK):
            continue
        for name in os.listdir(dir):
            subdir = os.path.join(dir, name)
            if not os.path.isdir(subdir) or not os.access(subdir, os.R_OK):
                continue
            for subname in os.listdir(subdir):
                path = os.path.join(subdir, subname)
                if utils.is_sockfile(path):
                    paths.append(path)
                    break  # We only expect 1 socket file per DB, so get out.
    # Try the default locations.
    for sockfile in DEFAULT_SOCKFILES:
        if not utils.is_sockfile(sockfile):
            continue
        paths.append(sockfile)
    return paths


def find_databases(dbs=None):
    """Returns a map of dbname (string) to DB instances to monitor.

    Args:
        dbs: A map of dbname (string) to DB instances already monitored.
             This map will be modified in place if it's not None.
    """
    sockfiles = find_sockfiles()
    if dbs is None:
        dbs = {}
    for sockfile in sockfiles:
        dbname = get_dbname(sockfile)
        if dbname in dbs:
            continue
        if not dbname:
            continue
        try:
            db = mysql_connect(sockfile)
            cursor = db.cursor()
            cursor.execute("SELECT VERSION()")
        except (EnvironmentError, EOFError, RuntimeError, socket.error,
                MySQLdb.MySQLError), e:
            utils.err("Couldn't connect to %s: %s" % (sockfile, e))
            continue
        version = cursor.fetchone()[0]
        dbs[dbname] = DB(sockfile, dbname, db, cursor, version)
    return dbs


def find_schemas(db):
    """Return a sequence of database schemas within the given db."""
    db_list_query = """
    SELECT
        SCHEMA_NAME
    FROM
        information_schema.schemata
    WHERE
        SCHEMA_NAME NOT IN ('mysql', 'performance_schema', 'information_schema', 'sys')
    """
    return db.query(db_list_query)


def now():
    return int(time.time())


def is_yes(s):
    if s.lower() == "yes":
        return 1
    return 0


def get_role_status():
    ms_checker_host = "localhost:3300"
    command_is_salve = "curl " + ms_checker_host + "/checkSlave"
    s, o = commands.getstatusoutput(command_is_salve)
    if o == "" or s != 0:
        utils.err("Error checking mysql role, status %s" % s)
    elif s == 0:
        utils.err("INFO: status msg: %s" % o)
        if "not" not in o.lower():
            return 1

    return 0


def get_disk_stats():
    path = '/data/mysql'
    min = '1048576'
    command_get_disk = "du -a %s |awk \"\$1 > %s {print}\"" % (path, min)
    s, o = commands.getstatusoutput(command_get_disk)
    if o == "" or s != 0:
        utils.err("Error checking mysql server disk status %s" % s)
    elif s == 0:
        # results = reduce(list.__add__, list(map(lambda x: x.split("\n"), o)))
        # stripped = list(map(lambda x: x.strip(), splittedEnter))
        # metrics = list(filter(lambda x: x.startswith(metricsPrefix), stripped))
        results = o.split("\n")
        disk_stats = {}
        for path_stats in results:
            path_stats_split = path_stats.split("\t")
            try:
                disk_stats[path_stats_split[1]] = path_stats_split[0]
            except:
                pass
        return disk_stats


def print_metric(db, ts, metric, value, tags=""):
    master_slave_tag = ' is_master=%s is_slave=%s is_dump_server=%s ' % (db.is_master, db.is_slave, db.is_dump_server)
    tags = '%s%s' % (master_slave_tag, tags)
    print "mysql.%s %d %s schema=%s%s" % (metric, ts, value, db.dbname, tags)


def collect_loop(collect_func, collect_interval, args):
    """Collects and dumps stats from a MySQL server."""
    if not find_sockfiles():  # Nothing to monitor.
        return 13               # Ask tcollector to not respawn us.
    if MySQLdb is None:
        utils.err("error: Python module `MySQLdb' is missing")
        return 1

    last_db_refresh = now()
    dbs = find_databases()
    while True:
        ts = now()
        if ts - last_db_refresh >= DB_REFRESH_INTERVAL:
            find_databases(dbs)
            last_db_refresh = ts

        errs = []
        for dbname, db in dbs.iteritems():
            try:
                collect_func(db)
            except (EnvironmentError, EOFError, RuntimeError, socket.error,
                    MySQLdb.MySQLError), e:
                if isinstance(e, IOError) and e[0] == errno.EPIPE:
                    # Exit on a broken pipe.  There's no point in continuing
                    # because no one will read our stdout anyway.
                    return 2
                utils.err("error: failed to collect data from %s: %s" % (db, e))
                errs.append(dbname)

        for dbname in errs:
            del dbs[dbname]

        sys.stdout.flush()

        sleep_seconds = collect_interval - (now() - ts)
        if sleep_seconds > 0:
            utils.err("INFO: sleep %d seconds before next collect." % sleep_seconds)
            time.sleep(sleep_seconds)
        else:
            utils.err("ERROR: one round of collect took %d seconds! "
                      "No sleep before next round of collect." %
                      (collect_interval - sleep_seconds))
