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
"""Collector for MySQL query stats from performance_schema."""

import datetime
import re
import sys
from collections import defaultdict

try:
    import MySQLdb
except ImportError:
    MySQLdb = None  # This is handled gracefully in main()

from collectors.lib.mysql_utils import (
    collect_loop,
    now,
    print_metric,
    to_dict,
)

COLLECTION_INTERVAL = 60  # seconds
MAX_NUM_METRICS = 200
MAX_STORAGE = 100000

def current_ts():
    return datetime.datetime.now().isoformat(sep=' ')[:19]


last_collection_ts = current_ts()
last_metrics = defaultdict(dict)

# Due to bug https://bugs.mysql.com/bug.php?id=79533, we can NOT assume
# the performance_schema.events_statements_summary_by_digest table
# has unique (schema_name, digest).

STMT_BY_DIGEST_QUERY_NEW = """
SELECT
    ifnull(SCHEMA_NAME, 'NONE') as SCHEMA_NAME,
    DIGEST,
    DIGEST_TEXT,
    SUM(COUNT_STAR) AS COUNT_STAR,
    SUM(SUM_TIMER_WAIT) AS SUM_TIMER_WAIT,
    SUM(SUM_ROWS_AFFECTED) AS SUM_ROWS_AFFECTED,
    SUM(SUM_ROWS_SENT) AS SUM_ROWS_SENT,
    SUM(SUM_ROWS_EXAMINED) AS SUM_ROWS_EXAMINED
FROM (
    SELECT
        *
    FROM
        performance_schema.events_statements_summary_by_digest
    WHERE
        SCHEMA_NAME NOT IN ('mysql', 'performance_schema', 'information_schema') AND
        LAST_SEEN >= '%(last_collection_ts)s'
) Q
GROUP BY
    Q.SCHEMA_NAME,
    Q.DIGEST,
    Q.DIGEST_TEXT
LIMIT %(limit)d;
"""

METRICS = [
    'count_star',
    'sum_timer_wait',
    'sum_rows_affected',
    'sum_rows_sent',
    'sum_rows_examined',
]

SUPPORTED_COMMANDS = frozenset([
    'ALTER',
    'BEGIN',
    'CHANGE',
    'COMMIT',
    'CREATE',
    'DELETE',
    'DESC',
    'DROP',
    'FLUSH',
    'GRANT',
    'INSERT',
    'KILL',
    'REPLACE',
    'SELECT',
    'SET',
    'SHOW',
    'START',
    'TRUNCATE',
    'UNION',
    'UPDATE',
])

DDL_DML_COMMANDS = frozenset([
    'ALTER',
    'BEGIN',
    'CHANGE',
    'COMMIT',
    'CREATE',
    'DELETE',
    'DROP',
    'FLUSH',
    'GRANT',
    'INSERT',
    'REPLACE',
    'SET',
    'START',
    'TRUNCATE',
    'UPDATE',
])


def get_command_from_digest_text(digest_text):
    """Extract command, ex. SELECT, DELETE, etc., from the digest_text."""
    cmd = digest_text.split(' ')[0]
    if cmd in SUPPORTED_COMMANDS:
        return cmd

    if cmd == '(':
        # Handle '(...) UNION (...)' query
        mo = re.match(u'[^\(]*\([^\(]+\) *([^\( ]+).*', digest_text)
        if mo:
            return mo.group(1)
    return 'UNKNOWN'


def should_skip_collect(db, command):
    """Skip collecting DDL and DML queries on slaves."""
    if db.is_slave and command in DDL_DML_COMMANDS:
        return True
    return False


def collect(db):
    """Collects and prints stats about the given DB instance."""
    global last_collection_ts
    global last_metrics

    # update master/slave status.
    db.check_set_master()
    db.check_set_slave()

    ts = now()
    collection_ts = current_ts()
    stmt_by_digest_new = db.query(
        STMT_BY_DIGEST_QUERY_NEW % {
            'last_collection_ts': last_collection_ts,
            'limit': MAX_STORAGE
        }
    )
    last_collection_ts = collection_ts

    count = 0
    # {metric_name: {tags: metric_value}}
    all_metrics = defaultdict(dict)
    for row in stmt_by_digest_new:
        stmt_summary = to_dict(db, row)
        digest = stmt_summary['digest']
        command = get_command_from_digest_text(stmt_summary['digest_text'])
        skip = should_skip_collect(db, command)
        if skip:
            continue

        # create tags
        def create_tags(digest):
            return " schema_name=%s digest=%s cmd=%s" % (
                stmt_summary['schema_name'], digest, command
            )

        tags = create_tags(digest)

        for name in METRICS:
            all_metrics[name][tags] = long(stmt_summary[name])
            count += 1

    if len(last_metrics.keys()) == 0:
        last_metrics = all_metrics
        print >> sys.stderr, "Initialized", count, "digest metrics"
        return

    # Sort metrics in descending order of deltas
    count2 = 0
    for name in METRICS:
        deltas = map(
            lambda x: [x[0], max(0, x[1] - last_metrics[name].get(x[0], 0))],
            all_metrics[name].items())
        deltas = sorted(deltas, key=lambda x: x[1], reverse=True)[:MAX_NUM_METRICS]
        first = True
        for tags, value in deltas:
            if value > 0 and tags in last_metrics[name]:
                print_metric(db, ts, "perf_schema.stmt_by_digest.%s.all" % name, all_metrics[name][tags], tags)
                count2 += 1
                if first:
                    print >> sys.stderr, db, ts, "perf_schema.stmt_by_digest.%s.all" % name, all_metrics[name][tags], tags
                    first = False
            elif value <= 0:
                break

        for key in all_metrics[name].keys():
            if key in last_metrics[name]:
                last_metrics[name].pop(key)

        num_extra = max(0, len(last_metrics[name].keys()) + len(all_metrics[name].keys()) - MAX_STORAGE)
        if num_extra > 0:
            extra_keys = last_metrics[name].keys()[:num_extra]
            for key in extra_keys:
                last_metrics[name].pop(key)
            num_extra = len(extra_keys)

        last_metrics[name].update(all_metrics[name])

    print >> sys.stderr, count, "collected", count2, "sent", num_extra, "evicted"


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(collect_loop(collect, COLLECTION_INTERVAL, sys.argv))
