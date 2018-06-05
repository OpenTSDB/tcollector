#!/usr/bin/env python
# -*- coding: utf-8 -*-
import errno
import re
import socket
import sys
import time

try:
    import MySQLdb
except ImportError:
    MySQLdb = None  # This is handled gracefully in main()

from collectors.etc import (
    proxysql_conf,
)
from collectors.lib import utils


CONNECT_TIMEOUT = 2  # seconds
MAX_QUERY_RETRY = 3
INSTANCE_REFRESH_INTERVAL = 60  # seconds


class ProxySQL(object):
    """Represents a ProxySQL instance (as we can monitor more than 1)."""

    def __init__(self, host, port, name, db, cursor, version):
        """Constructor.

        Args:
            host, port: The admin endpoint of the ProxySQL instance.
            name: Name of the ProxySQL instance.
            db: A MySQLdb connection opened to that ProxySQL instance.
            cursor: A cursor acquired from that connection.
            version: What version is this ProxySQL running (from `SELECT VERSION()').
        """
        self.host = host
        self.port = port
        self.name = name
        self.db = db
        self.cursor = cursor
        self.version = version

        version = version.split(".")
        try:
            self.major = int(version[0])
            self.medium = int(version[1])
        except (ValueError, IndexError):
            self.major = self.medium = 0

    def __str__(self):
        return "ProxySQL(%r, %r, %r, version=%r)" % (
            self.host, self.port, self.name, self.version
        )

    def __repr__(self):
        return self.__str__()

    def __del__(self):
        self.close()

    def query(self, sql):
        """Executes the given SQL statement and returns a sequence of rows."""
        assert self.cursor, "%s already closed?" % (self,)
        for _ in range(MAX_QUERY_RETRY):
            try:
                self.cursor.execute(sql)
                return self.cursor.fetchall()
            except MySQLdb.OperationalError, (errcode, msg):
                if errcode != 2006:  # "MySQL server has gone away"
                    raise
                self._reconnect()
        return []

    def to_dict(self, row):
        """Transforms a row (returned by DB.query) into a dict keyed by column names.
        """
        d = {}
        for i, field in enumerate(self.cursor.description):
            column = field[0].lower()  # Lower-case to normalize field names.
            d[column] = row[i]
        return d

    def close(self):
        """Closes the connection to this ProxySQL instance."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.db:
            self.db.close()
            self.db = None

    def _reconnect(self):
        """Reconnects to this ProxySQL instance."""
        self.close()
        self.db = proxysql_admin_connect(self.host, self.port)
        self.cursor = self.db.cursor()


# Invalid pattern applies to both tag key and value.
INVALID_TAG_CHAR = re.compile(r'[^-_./a-zA-Z0-9]+')

def export_metric(proxysql, name, tags, ts, val):
    def sanitize_tag(key_or_val):
        return re.sub(INVALID_TAG_CHAR, '-', key_or_val)

    if not tags:
        tags = {}
    tags.update(
        {'admin_port': str(proxysql.port)}
    )

    new_tags = {}
    for k, v in tags.iteritems():
        sanitized_key = sanitize_tag(k)
        sanitized_val = sanitize_tag(v)
        if sanitized_key and sanitized_val:
            new_tags[sanitized_key] = sanitized_val
    tag_str = ' '.join(
        '%s=%s' % (k, v) for k, v in new_tags.iteritems()
    )

    print '%s %d %s %s' % (name, ts, val, tag_str)


def now():
    return int(time.time())


def proxysql_admin_connect(host, port):
    """Connects to the ProxySQL admin interface using the specified (host, port)."""
    user, passwd = proxysql_conf.get_user_password(host, port)
    return MySQLdb.connect(host=host, port=port,
                           connect_timeout=CONNECT_TIMEOUT,
                           user=user, passwd=passwd)


def find_proxysql_instances(instances=None):
    """Returns a map of name to ProxySQL instances to monitor.

    Args:
        instances: A map of name to ProxySQL instances already monitored.
                   This map will be modified in place if it's not None.
    """
    if instances is None:
        instances = {}
    for host, port in proxysql_conf.get_host_ports():
        name = str(port)
        if name in instances:
            continue
        try:
            inst = proxysql_admin_connect(host, port)
            cursor = inst.cursor()
            cursor.execute("SELECT VERSION()")
        except (EnvironmentError, EOFError, RuntimeError,
                socket.error, MySQLdb.MySQLError), e:
            utils.err('Could not connect to instance at (%s, %s): %s',
                      host, port, e)
            continue
        version = cursor.fetchone()[0]
        instances[name] = ProxySQL(host, port, name, inst, cursor, version)
    return instances


def collect_loop(collect_func, collect_interval, args):
    """Collects and dumps stats from a ProxySQL instance."""
    instances = find_proxysql_instances()
    last_instance_refresh = now()
    if not instances:  # Nothing to monitor.
        return 13               # Ask tcollector to not respawn us.
    if MySQLdb is None:
        utils.err('Python module MySQLdb is missing')
        return 1

    while True:
        ts = now()
        if ts - last_instance_refresh >= INSTANCE_REFRESH_INTERVAL:
            find_proxysql_instances(instances)
            last_instance_refresh = ts

        errs = []
        for name, inst in instances.iteritems():
            try:
                collect_func(inst)
            except (EnvironmentError,
                    EOFError,
                    RuntimeError,
                    socket.error,
                    MySQLdb.MySQLError), e:
                if isinstance(e, IOError) and e[0] == errno.EPIPE:
                    # Exit on a broken pipe.  There's no point in continuing
                    # because no one will read our stdout anyway.
                    return 2
                utils.err('failed to collect data from %s: %s' % (inst, e))
                errs.append(name)

        for name in errs:
            del instances[name]

        sys.stdout.flush()

        sleep_seconds = collect_interval - (now() - ts)
        if sleep_seconds > 0:
            utils.err('INFO: sleep %d seconds before next collect.' % sleep_seconds)
            time.sleep(sleep_seconds)
        else:
            utils.err('one round of collect took %d seconds! '
                      'No sleep before next round of collect.' %
                      (collect_interval - sleep_seconds))
