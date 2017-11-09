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
"""Collector for MySQL global and innodb status."""

import errno
import re
import socket
import sys
import time

try:
    import MySQLdb
except ImportError:
    MySQLdb = None  # This is handled gracefully in main()

from collectors.lib import utils
from collectors.lib.mysql_utils import (
    DB_REFRESH_INTERVAL,
    is_yes,
    now,
    print_metric,
    to_dict,
    find_databases,
    find_sockfiles,
)

COLLECTION_INTERVAL = 15  # seconds


def collectInnodbStatus(db):
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

    start_merged_line = False
    for line in innodb_status.split("\n"):
        # SEMAPHORES
        m = match("OS WAIT ARRAY INFO: reservation count (\d+), signal count (\d+)")
        if m:
            print_metric(db, ts, "innodb.oswait_array.reservation_count", m.group(1))
            print_metric(db, ts, "innodb.oswait_array.signal_count", m.group(2))
            continue
        m = match("Mutex spin waits (\d+), rounds (\d+), OS waits (\d+)")
        if m:
            print_metric(db, ts, "innodb.locks.spin_waits", m.group(1), " type=mutex")
            print_metric(db, ts, "innodb.locks.rounds", m.group(2), " type=mutex")
            print_metric(db, ts, "innodb.locks.os_waits", m.group(3), " type=mutex")
            continue
        m = match("RW-shared spins (\d+), OS waits (\d+);"
                  " RW-excl spins (\d+), OS waits (\d+)")
        if m:
            print_metric(db, ts, "innodb.locks.spin_waits", m.group(1), " type=rw-shared")
            print_metric(db, ts, "innodb.locks.os_waits", m.group(2), " type=rw-shared")
            print_metric(db, ts, "innodb.locks.spin_waits", m.group(3), " type=rw-exclusive")
            print_metric(db, ts, "innodb.locks.os_waits", m.group(4), " type=rw-exclusive")
            continue
        # GG 20141015 - RW-shared and RW-excl got separate lines and rounds in 5.5+
        m = match("RW-shared spins (\d+), rounds (\d+), OS waits (\d+)")
        if m:
            print_metric(db, ts, "locks.spin_waits", m.group(1), " type=rw-shared")
            print_metric(db, ts, "locks.rounds", m.group(2), " type=rw-shared")
            print_metric(db, ts, "locks.os_waits", m.group(3), " type=rw-shared")
            continue
        m = match("RW-excl spins (\d+), rounds (\d+), OS waits (\d+)")
        if m:
            print_metric(db, ts, "locks.spin_waits", m.group(1), " type=rw-exclusive")
            print_metric(db, ts, "locks.rounds", m.group(2), " type=rw-exclusive")
            print_metric(db, ts, "locks.os_waits", m.group(3), " type=rw-exclusive")
            continue
        # INSERT BUFFER AND ADAPTIVE HASH INDEX
        # TODO(tsuna): According to the code in ibuf0ibuf.c, this line and
        # the following one can appear multiple times.  I've never seen this.
        # If that happens, we need to aggregate the values here instead of
        # printing them directly.
        m = match("Ibuf: size (\d+), free list len (\d+), seg size (\d+), (\d+) merges")
        if m:
            print_metric(db, ts, "innodb.ibuf.size", m.group(1))
            print_metric(db, ts, "innodb.ibuf.free_list_len", m.group(2))
            print_metric(db, ts, "innodb.ibuf.seg_size", m.group(3))
            print_metric(db, ts, "innodb.ibuf.merges", m.group(4))
            continue
        m = match("merged operations:")
        if m:
            start_merged_line = True
            continue
        if start_merged_line:
            start_merged_line = False
            m = match("\s*insert (\d+), delete mark (\d+), delete (\d+)")
            if m:
                print_metric(db, ts, "innodb.ibuf.merged_operations",
                             int(m.group(1)) + int(m.group(2)) + int(m.group(3)))
                continue
        # ROW OPERATIONS
        m = match("\d+ queries inside InnoDB, (\d+) queries in queue")
        if m:
            print_metric(db, ts, "innodb.queries_queued", m.group(1))
            continue
        m = match("(\d+) read views open inside InnoDB")
        if m:
            print_metric(db, ts, "innodb.opened_read_views", m.group(1))
            continue
        # TRANSACTION
        m = match("History list length (\d+)")
        if m:
            print_metric(db, ts, "innodb.history_list_length", m.group(1))
            continue


def collect(db):
    """Collects and prints stats about the given DB instance."""

    ts = now()

    has_innodb = False
    if db.isShowGlobalStatusSafe():
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
            print_metric(db, ts, metric, value)

    if has_innodb:
        collectInnodbStatus(db)

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
            print_metric(db, ts, "innodb.locks", wait_count, " mutex=" + mutex)

    ts = now()

    mysql_attached_slaves = db.query("SHOW SLAVE HOSTS")
    if mysql_attached_slaves:
        db.setMaster(True)
        print_metric(db, ts, "master.attached_slaves", len(mysql_attached_slaves))
    else:
        db.setMaster(False)

    mysql_slave_status = db.query("SHOW SLAVE STATUS")
    if mysql_slave_status:
        # update master/slave status of the DB
        db.setSlave(True)
        slave_status = to_dict(db, mysql_slave_status[0])
        master_host = slave_status["master_host"]
    else:
        db.setSlave(False)
        master_host = None

    if master_host and master_host != "None":
        sbm = slave_status.get("seconds_behind_master")
        if isinstance(sbm, (int, long)):
            print_metric(db, ts, "slave.seconds_behind_master", sbm)
        print_metric(db, ts, "slave.bytes_executed", slave_status["exec_master_log_pos"])
        print_metric(db, ts, "slave.bytes_relayed", slave_status["read_master_log_pos"])
        print_metric(db, ts, "slave.thread_io_running",
                     is_yes(slave_status["slave_io_running"]))
        print_metric(db, ts, "slave.thread_sql_running",
                     is_yes(slave_status["slave_sql_running"]))

    states = {}  # maps a connection state to number of connections in that state
    for row in db.query("SHOW PROCESSLIST"):
        id, user, host, db_, cmd, time, state = row[:7]
        states[cmd] = states.get(cmd, 0) + 1
    for state, count in states.iteritems():
        state = state.lower().replace(" ", "_")
        print_metric(db, ts, "connection_states", count, " state=%s" % state)


def main(args):
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
                collect(db)
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
        time.sleep(COLLECTION_INTERVAL)


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main(sys.argv))
