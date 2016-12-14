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
"""Collector for MySQL."""

import os
import re
import socket
import time

try:
    import MySQLdb
except ImportError:
    MySQLdb = None  # This is handled gracefully in main()

from collectors.lib import utils
from collectors.lib.collectorbase import CollectorBase

CONNECT_TIMEOUT = 2  # seconds
# How frequently we try to find new databases.
DB_REFRESH_INTERVAL = 60  # seconds
# Usual locations where to find the default socket file.
DEFAULT_SOCKFILES = {"/tmp/mysql.sock", "/var/lib/mysql/mysql.sock", "/var/run/mysqld/mysqld.sock"}
# Directories under which to search additional socket files.
SEARCH_DIRS = ["/var/lib/mysql"]


class DB(object):
    """Represents a MySQL server (as we can monitor more than 1 MySQL)."""

    def __init__(self, sockfile, dbname, db, cursor, version, user, passwd):
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
        self.user = user
        self.passwd = passwd

        version = version.split(".")
        try:
            self.major = int(version[0])
            self.medium = int(version[1])
        except (ValueError, IndexError):
            self.major = self.medium = 0

    def __str__(self):
        return "DB(%r, %r, version=%r)" % (self.sockfile, self.dbname,
                                           self.version)

    def __repr__(self):
        return self.__str__()

    def is_show_global_status_safe(self):
        """Returns whether or not SHOW GLOBAL STATUS is safe to run."""
        # We can't run SHOW GLOBAL STATUS on versions prior to 5.1 because it
        # locks the entire database for too long and severely impacts traffic.
        return self.major > 5 or (self.major == 5 and self.medium >= 1)

    def query(self, sql):
        """Executes the given SQL statement and returns a sequence of rows."""
        assert self.cursor, "%s already closed?" % (self,)
        try:
            self.cursor.execute(sql)
        except MySQLdb.OperationalError, (errcode, msg):
            if errcode != 2006:  # "MySQL server has gone away"
                raise
            self._reconnect()
        return self.cursor.fetchall()

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
        self.db = mysql_connect(self.sockfile, self.user, self.passwd)
        self.cursor = self.db.cursor()


def mysql_connect(sockfile, user, passwd):
    """Connects to the MySQL server using the specified socket file."""
    return MySQLdb.connect(unix_socket=sockfile,
                           connect_timeout=CONNECT_TIMEOUT,
                           user=user, passwd=passwd)


def todict(db, row):
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


def find_sockfiles():
    """Returns a list of paths to socket files to monitor."""
    paths = []
    # Look for socket files.
    for thedir in SEARCH_DIRS:
        if not os.path.isdir(thedir) or not os.access(thedir, os.R_OK):
            continue
        for name in os.listdir(thedir):
            subdir = os.path.join(thedir, name)
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


def now():
    return int(time.time())


def isyes(s):
    if s.lower() == "yes":
        return 1
    return 0


class Mysql(CollectorBase):
    def __init__(self, config, logger, readq):
        super(Mysql, self).__init__(config, logger, readq)
        if MySQLdb is None:
            raise ImportError("unable to load Python module `MySQLdb'")
        self.connection_user = self.get_config("user", "cloudwiz_user")
        self.connection_pass = self.get_config("pass", "cloudwiz_pass")
        self.last_db_refresh = 0
        self.dbs = {}

    def __call__(self):
        ts = now()
        if ts - self.last_db_refresh >= DB_REFRESH_INTERVAL:
            self.find_databases(self.dbs)
            self.last_db_refresh = ts
        if self.dbs is None:
            self.log_info("db is none. abort collection")
            return

        errs = []
        for dbname, db in self.dbs.iteritems():
            try:
                self.collect(db)
            except (EnvironmentError, EOFError, RuntimeError, socket.error, MySQLdb.MySQLError):
                self.log_exception("error: failed to collect data from %s" % db)
                errs.append(dbname)

        for dbname in errs:
            del self.dbs[dbname]

    def collect(self, db):
        """Collects and prints stats about the given DB instance."""
        ts = now()
        has_innodb = False
        if db.is_show_global_status_safe():
            for metric, value in db.query("SHOW GLOBAL STATUS"):
                try:
                    if "." in value:
                        value = float(value)
                    else:
                        value = int(value)
                except ValueError:
                    continue
                metric = metric.lower()
                has_innodb = has_innodb or metric.startswith("innodb")
                self.printmetric(metric, ts, value, db)

        if has_innodb:
            self.collect_innodb_status(db)

        if has_innodb and False:  # Disabled because it's too expensive for InnoDB.
            waits = {}  # maps a mutex name to the number of waits
            ts = now()
            for engine, mutex, status in db.query("SHOW ENGINE INNODB MUTEX"):
                if not status.startswith("os_waits"):
                    continue
                m = re.search("&(\w+)(?:->(\w+))?$", mutex)
                if not m:
                    continue
                mutex, kind = m.groups()
                if kind:
                    mutex += "." + kind
                wait_count = int(status.split("=", 1)[1])
                waits[mutex] = waits.get(mutex, 0) + wait_count
            for mutex, wait_count in waits.iteritems():
                self.printmetric("innodb.locks", ts, wait_count, db, " mutex=" + mutex)

        ts = now()
        mysql_slave_status = db.query("SHOW SLAVE STATUS")
        if mysql_slave_status:
            slave_status = todict(db, mysql_slave_status[0])
            master_host = slave_status["master_host"]
        else:
            master_host = None

        if master_host and master_host != "None":
            sbm = slave_status.get("seconds_behind_master")
            if isinstance(sbm, (int, long)):
                self.printmetric("slave.seconds_behind_master", ts, sbm, db)
            self.printmetric("slave.bytes_executed", ts, slave_status["exec_master_log_pos"], db)
            self.printmetric("slave.bytes_relayed", ts, slave_status["read_master_log_pos"], db)
            self.printmetric("slave.thread_io_running", ts, isyes(slave_status["slave_io_running"]), db)
            self.printmetric("slave.thread_sql_running", ts, isyes(slave_status["slave_sql_running"]), db)

        states = {}  # maps a connection state to number of connections in that state
        for row in db.query("SHOW PROCESSLIST"):
            pid, user, host, db_, cmd, t, state = row[:7]
            states[cmd] = states.get(cmd, 0) + 1
        for state, count in states.iteritems():
            state = state.lower().replace(" ", "_")
            self.printmetric("connection_states", ts, count, db, " state=%s" % state)

    def collect_innodb_status(self, db):
        """Collects and prints InnoDB stats about the given DB instance."""
        ts = now()

        innodb_status = db.query("SHOW ENGINE INNODB STATUS")[0][2]
        m = re.search("^(\d{6}\s+\d{1,2}:\d\d:\d\d) INNODB MONITOR OUTPUT$",
                      innodb_status, re.M)
        if m:  # If we have it, try to use InnoDB's own timestamp.
            ts = int(time.mktime(time.strptime(m.group(1), "%y%m%d %H:%M:%S")))

        line = None

        def match(regexp):
            return re.match(regexp, line)

        for line in innodb_status.split("\n"):
            # SEMAPHORES
            m = match("OS WAIT ARRAY INFO: reservation count (\d+), signal count (\d+)")
            if m:
                self.printmetric("innodb.oswait_array.reservation_count", ts, m.group(1), db)
                self.printmetric("innodb.oswait_array.signal_count", ts, m.group(2), db)
                continue
            m = match("Mutex spin waits (\d+), rounds (\d+), OS waits (\d+)")
            if m:
                self.printmetric("innodb.locks.spin_waits", ts, m.group(1), db, " type=mutex")
                self.printmetric("innodb.locks.rounds", ts, m.group(2), db, " type=mutex")
                self.printmetric("innodb.locks.os_waits", ts, m.group(3), db, " type=mutex")
                continue
            m = match("RW-shared spins (\d+), OS waits (\d+);"
                      " RW-excl spins (\d+), OS waits (\d+)")
            if m:
                self.printmetric("innodb.locks.spin_waits", ts, m.group(1), db, " type=rw-shared")
                self.printmetric("innodb.locks.os_waits", ts, m.group(2), db, " type=rw-shared")
                self.printmetric("innodb.locks.spin_waits", ts, m.group(3), db, " type=rw-exclusive")
                self.printmetric("innodb.locks.os_waits", ts, m.group(4), db, " type=rw-exclusive")
                continue
            # GG 20141015 - RW-shared and RW-excl got separate lines and rounds in 5.5+
            m = match("RW-shared spins (\d+), rounds (\d+), OS waits (\d+)")
            if m:
                self.printmetric("locks.spin_waits", ts, m.group(1), db, " type=rw-shared")
                self.printmetric("locks.rounds", ts, m.group(2), db, " type=rw-shared")
                self.printmetric("locks.os_waits", ts, m.group(3), db, " type=rw-shared")
                continue
            m = match("RW-excl spins (\d+), rounds (\d+), OS waits (\d+)")
            if m:
                self.printmetric("locks.spin_waits", ts, m.group(1), db, " type=rw-exclusive")
                self.printmetric("locks.rounds", ts, m.group(2), db, " type=rw-exclusive")
                self.printmetric("locks.os_waits", ts, m.group(3), db, " type=rw-exclusive")
                continue
            # INSERT BUFFER AND ADAPTIVE HASH INDEX
            # TODO(tsuna): According to the code in ibuf0ibuf.c, this line and
            # the following one can appear multiple times.  I've never seen this.
            # If that happens, we need to aggregate the values here instead of
            # printing them directly.
            m = match("Ibuf: size (\d+), free list len (\d+), seg size (\d+),")
            if m:
                self.printmetric("innodb.ibuf.size", ts, m.group(1), db)
                self.printmetric("innodb.ibuf.free_list_len", ts, m.group(2), db)
                self.printmetric("innodb.ibuf.seg_size", ts, m.group(3), db)
                continue
            m = match("(\d+) inserts, (\d+) merged recs, (\d+) merges")
            if m:
                self.printmetric("innodb.ibuf.inserts", ts, m.group(1), db)
                self.printmetric("innodb.ibuf.merged_recs", ts, m.group(2), db)
                self.printmetric("innodb.ibuf.merges", ts, m.group(3), db)
                continue
            # ROW OPERATIONS
            m = match("\d+ queries inside InnoDB, (\d+) queries in queue")
            if m:
                self.printmetric("innodb.queries_queued", ts, m.group(1), db)
                continue
            m = match("(\d+) read views open inside InnoDB")
            if m:
                self.printmetric("innodb.opened_read_views", ts, m.group(1), db)
                continue
            # TRANSACTION
            m = match("History list length (\d+)")
            if m:
                self.printmetric("innodb.history_list_length", ts, m.group(1), db)
                continue

    def printmetric(self, metric, ts, value, db, tags=""):
        self._readq.nput("mysql.%s %d %s schema=%s%s" % (metric, ts, value, db.dbname, tags))

    def find_databases(self, dbs=None):
        """Returns a map of dbname (string) to DB instances to monitor.

      Args:
        dbs: A map of dbname (string) to DB instances already monitored.
          This map will be modified in place if it's not None.
      """
        sockfiles = find_sockfiles()
        if not sockfiles:
            raise IOError("unable to find mysql socket file")
        if dbs is None:
            dbs = {}
        for sockfile in sockfiles:
            dbname = self.get_dbname(sockfile)
            if dbname in dbs:
                continue
            if not dbname:
                continue
            try:
                db = mysql_connect(sockfile, self.connection_user, self.connection_pass)
                cursor = db.cursor()
                cursor.execute("SELECT VERSION()")
            except (EnvironmentError, EOFError, RuntimeError, socket.error, MySQLdb.MySQLError):
                self.log_exception("Couldn't connect to %s." % sockfile)
                continue
            version = cursor.fetchone()[0]
            dbs[dbname] = DB(sockfile, dbname, db, cursor, version, self.connection_user, self.connection_pass)
        return dbs

    def get_dbname(self, sockfile):
        """Returns the name of the DB based on the path to the socket file."""
        if sockfile in DEFAULT_SOCKFILES:
            return "default"
        m = re.search("/mysql-(.+)/[^.]+\.sock$", sockfile)
        if not m:
            self.log_error("error: couldn't guess the name of the DB for " + sockfile)
            return None
        return m.group(1)

if __name__ == "__main__":
    from Queue import Queue
    mysql_inst = Mysql(None, None, Queue())
    mysql_inst()
