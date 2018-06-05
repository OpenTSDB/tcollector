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
"""Collector for ProxySQL status."""

import sys
import time

from collectors.lib import utils
from collectors.lib.proxysql_utils import (
    collect_loop,
    export_metric,
    now,
)

COLLECTION_INTERVAL = 30  # seconds
METRIC_NAME_PREFIX = 'proxysql'


def round_timestamp_to_mark(ts=None):
    """Round timestamp down to the nearest 30 seconds mark.

    Note: ts, if passed in, should be in seconds.
    """
    interval = int(COLLECTION_INTERVAL)
    if not ts:
        ts = time.time()
    return int(ts) / interval * interval


class StatsTable(object):
    TABLE_NAME = None
    COLUMNS = []
    SELECT_LIMIT = 1000

    def __init__(self, db, ts_func=None):
        self.db = db
        if ts_func:
            self.ts_func = ts_func
        else:
            self.ts_func = self.get_timestamp

    def value_func(self, value):
        try:
            return int(value)
        except:
            utils.err('Non numeric metric value: %s. Ignore.' % value)
        return 0

    def get_key_tag_values(self, row):
        """Return list of (metric_name, tags, value) tuples, based on the row."""
        raise NotImplementedError

    def get_timestamp(self, row):
        return now()

    def get_rows(self):
        sql = 'SELECT %s FROM %s LIMIT %s' % (
            ','.join(self.COLUMNS),
            self.TABLE_NAME,
            self.SELECT_LIMIT,
        )
        rows = []
        for row in self.db.query(sql):
            rows.append(self.db.to_dict(row))
        return rows

    def get_metrics(self):
        """Return list of (metric_name, tags, timestamp, value) tuples."""
        metrics = []
        for row in self.get_rows():
            for key, tag, val in self.get_key_tag_values(row):
                metrics.append(
                    (key, tag, self.ts_func(row), val)
                )
        return metrics


class StatsVariableTable(StatsTable):
    """StatsVariableTable generates one metric per row.

    NOTE:
    - The KEY_COLUMN value as part of the metric name.
    - No tags for the generated metrics.

    Example:
    mysql> select * from stats_mysql_global;
    +------------------------------+----------------+
    | Variable_Name                | Variable_Value |
    +------------------------------+----------------+
    | ProxySQL_Uptime              | 388574         |
    +------------------------------+----------------+
    """
    COLUMNS = [
        'variable_name',
        'variable_value',
    ]
    KEY_COLUMN = 'variable_name'
    VALUE_COLUMN = 'variable_value'

    def get_key_tag_values(self, row):
        key = '%s.%s.%s' % (
            METRIC_NAME_PREFIX,
            self.TABLE_NAME,
            str(row[self.KEY_COLUMN]).lower(),
        )
        val = self.value_func(row[self.VALUE_COLUMN])
        return [(key, None, val)]


class StatsMemoryMetrics(StatsVariableTable):
    TABLE_NAME = 'stats_memory_metrics'


class StatsMysqlGlobal(StatsVariableTable):
    TABLE_NAME = 'stats_mysql_global'


class StatsMultiValueTable(StatsTable):
    """StatsMultiValueTable generates multiple metrics per row with tags.

    NOTE:
    - Each column name in VALUE_COLUMNS is part of the metric name.
    - Each column name in TAG_COLUMNS adds a new tag.
    - Metric with value filtered out by the value_filter_func is dropped.
    - Derived metrics can be calculated from multiple value columns.

    Example:
    mysql> select * from stats_mysql_query_rules;
    +---------+------+
    | rule_id | hits |
    +---------+------+
    | 1       | 1465 |
    +---------+------+
    """
    TAG_COLUMNS = []
    VALUE_COLUMNS = []

    def value_filter_func(self, value):
        return value != 0

    def get_tags(self, row):
        tags = {}
        for tag_name in self.TAG_COLUMNS:
            tags[tag_name] = str(row[tag_name]).lower()
        return tags

    def get_derived_key_tag_values(self, row):
        return []

    def get_key_tag_values(self, row):
        tags = self.get_tags(row)

        results = []
        for val_column in self.VALUE_COLUMNS:
            val = self.value_func(row[val_column])
            if self.value_filter_func(val):
                key = '%s.%s.%s' % (
                    METRIC_NAME_PREFIX,
                    self.TABLE_NAME,
                    val_column,
                )
                results.append((key, tags, val))

        derived = self.get_derived_key_tag_values(row)
        if derived:
            results.extend(derived)

        return results


class StatsMysqlCommandsCounters(StatsMultiValueTable):
    TABLE_NAME = 'stats_mysql_commands_counters'
    COLUMNS = [
        'command',
        'total_time_us',
        'total_cnt',
        'cnt_100us',
        'cnt_500us',
        'cnt_1ms',
        'cnt_5ms',
        'cnt_10ms',
        'cnt_50ms',
        'cnt_100ms',
        'cnt_500ms',
        'cnt_1s',
        'cnt_5s',
        'cnt_10s',
        'cnt_infs',
    ]
    TAG_COLUMNS = [
        'command',
    ]
    VALUE_COLUMNS = [
        'total_time_us',
        'total_cnt',
        'cnt_100us',
        'cnt_500us',
        'cnt_1ms',
        'cnt_5ms',
        'cnt_10ms',
        'cnt_50ms',
        'cnt_100ms',
        'cnt_500ms',
        'cnt_1s',
        'cnt_5s',
        'cnt_10s',
        'cnt_infs',
    ]


class StatsMysqlConnectionPool(StatsMultiValueTable):
    TABLE_NAME = 'stats_mysql_connection_pool'
    COLUMNS = [
        'hostgroup',
        'srv_host',
        'srv_port',
        'status',
        'connused',
        'connfree',
        'connok',
        'connerr',
        'queries',
        'bytes_data_sent',
        'bytes_data_recv',
        'latency_us',
    ]
    TAG_COLUMNS = [
        'hostgroup',
        'srv_host',
        'status',
    ]
    VALUE_COLUMNS = [
        'connused',
        'connfree',
        'connok',
        'connerr',
        'queries',
        'bytes_data_sent',
        'bytes_data_recv',
        'latency_us',
    ]


class StatsMysqlQueryRules(StatsMultiValueTable):
    TABLE_NAME = 'stats_mysql_query_rules'
    COLUMNS = [
        'rule_id',
        'hits',
    ]
    TAG_COLUMNS = [
        'rule_id',
    ]
    VALUE_COLUMNS = [
        'hits',
    ]


class StatsMysqlUsers(StatsMultiValueTable):
    TABLE_NAME = 'stats_mysql_users'
    COLUMNS = [
        'username',
        'frontend_connections',
        'frontend_max_connections',
    ]
    TAG_COLUMNS = [
        'username',
    ]
    VALUE_COLUMNS = [
        'frontend_connections',
        'frontend_max_connections',
    ]

    def get_derived_key_tag_values(self, row):
        """Calculate percentage of used connections."""
        try:
            used_perc = int(row['frontend_connections']) * 100 / int(row['frontend_max_connections'])
        except:
            used_perc = 0

        return [(
            '%s.%s.%s' % (METRIC_NAME_PREFIX, self.TABLE_NAME, 'conn_used_percentage'),
            self.get_tags(row),
            used_perc,
        )]


class StatsProxysqlServersChecksums(StatsMultiValueTable):
    TABLE_NAME = 'stats_proxysql_servers_checksums'
    COLUMNS = [
        'hostname',
        'port',
        'name',
        'version',
        'epoch',
        'checksum',
        'changed_at',
        'updated_at',
    ]
    TAG_COLUMNS = [
        'hostname',
        'port',
        'name',
        'checksum',
    ]
    VALUE_COLUMNS = [
        'version',
        'epoch',
        'changed_at',
        'updated_at',
    ]


class StatsProxysqlServersMetrics(StatsMultiValueTable):
    TABLE_NAME = 'stats_proxysql_servers_metrics'
    COLUMNS = [
        'hostname',
        'port',
        'weight',
        'comment',
        'response_time_ms',
        'uptime_s',
        'last_check_ms',
        'queries',
        'client_connections_connected',
        'client_connections_created',
    ]
    TAG_COLUMNS = [
        'hostname',
        'port',
    ]
    VALUE_COLUMNS = [
        'response_time_ms',
        'uptime_s',
        'last_check_ms',
        'queries',
        'client_connections_connected',
        'client_connections_created',
    ]


STATS_TABLES = [
    StatsMemoryMetrics,
    StatsMysqlGlobal,
    StatsMysqlCommandsCounters,
    StatsMysqlConnectionPool,
    StatsMysqlQueryRules,
    StatsMysqlUsers,
    StatsProxysqlServersChecksums,
    StatsProxysqlServersMetrics,
]


class LogTable(StatsMultiValueTable):
    """Table that logs the timing of varioius operations."""
    START_TIME_COLUMN = 'time_start_us'
    PKEY_COLUMNS = [
        'hostname',
        'port',
    ]

    def get_timestamp(self, row):
        return int(int(row[self.START_TIME_COLUMN]) / 1e6)  # time in micro-seconds

    def get_rows(self):
        """Only select the latest row from the log table."""

        sql = '''
        SELECT
            %(columns)s
        FROM
            %(table_name)s p
        JOIN (
            SELECT
                %(pkey_columns)s, MAX(%(ts_column)s) AS %(ts_column)s
            FROM
                %(table_name)s
            GROUP BY
                %(pkey_columns)s
            LIMIT
                %(limit)d
        ) t
        ON
            %(on_clause)s
        LIMIT
            %(limit)d
        ''' % {
            'table_name': self.TABLE_NAME,
            'columns': ','.join('p.%s' % c for c in self.COLUMNS),
            'pkey_columns': ','.join(self.PKEY_COLUMNS),
            'ts_column': self.START_TIME_COLUMN,
            'on_clause': ' AND '.join(
                'p.%s=t.%s' % (c, c) for c in self.PKEY_COLUMNS + [self.START_TIME_COLUMN]
            ),
            'limit': self.SELECT_LIMIT,
        }

        rows = []
        for row in self.db.query(sql):
            rows.append(self.db.to_dict(row))
        return rows


class MysqlServerConnectLog(LogTable):
    TABLE_NAME = 'mysql_server_connect_log'
    PKEY_COLUMNS = [
        'hostname',
        'port',
    ]
    COLUMNS = [
        'hostname',
        'port',
        'time_start_us',
        'connect_success_time_us',
        'connect_error',
    ]
    TAG_COLUMNS = [
        'hostname',
        'port',
        'connect_error',
    ]
    VALUE_COLUMNS = [
        'connect_success_time_us',
    ]


class MysqlServerPingLog(LogTable):
    TABLE_NAME = 'mysql_server_ping_log'
    COLUMNS = [
        'hostname',
        'port',
        'time_start_us',
        'ping_success_time_us',
        'ping_error',
    ]
    TAG_COLUMNS = [
        'hostname',
        'port',
        'ping_error',
    ]
    VALUE_COLUMNS = [
        'ping_success_time_us',
    ]


class MysqlServerReadOnlyLog(LogTable):
    TABLE_NAME = 'mysql_server_read_only_log'
    COLUMNS = [
        'hostname',
        'port',
        'time_start_us',
        'success_time_us',
        'read_only',
        'error',
    ]
    TAG_COLUMNS = [
        'hostname',
        'port',
        'read_only',
        'error',
    ]
    VALUE_COLUMNS = [
        'success_time_us',
    ]


class MysqlServerReplicationLaglog(LogTable):
    TABLE_NAME = 'mysql_server_replication_lag_log'
    COLUMNS = [
        'hostname',
        'port',
        'time_start_us',
        'success_time_us',
        'repl_lag',
        'error',
    ]
    TAG_COLUMNS = [
        'hostname',
        'port',
        'error',
    ]
    VALUE_COLUMNS = [
        'success_time_us',
        'repl_lag',
    ]


LOG_TABLES = [
    MysqlServerConnectLog,
    MysqlServerPingLog,
    MysqlServerReadOnlyLog,
    MysqlServerReplicationLaglog,
]


def collect(db):
    """Collects and prints stats about the given ProxySQL instance."""
    ts = now()

    for table_cls in STATS_TABLES + LOG_TABLES:
        table_obj = table_cls(db, ts_func=lambda _: ts)
        for key, tags, metric_ts, val in table_obj.get_metrics():
            export_metric(db, key, tags, metric_ts, val)


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(collect_loop(collect, COLLECTION_INTERVAL, sys.argv))
