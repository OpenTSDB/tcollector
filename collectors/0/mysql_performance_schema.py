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

NUM_QUERY_SUMMARY_LIMIT = 10000
MAX_QUERY_LAST_SEEN_SECONDS = 86400

# Due to bug https://bugs.mysql.com/bug.php?id=79533, we can NOT assume
# the performance_schema.events_statements_summary_by_digest table
# has unique (schema_name, digest).
STMT_BY_DIGEST_QUERY = """
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
        LAST_SEEN > DATE_SUB(NOW(), INTERVAL %(last_seen_seconds)d SECOND)
) Q
GROUP BY
    Q.SCHEMA_NAME,
    Q.DIGEST,
    Q.DIGEST_TEXT
ORDER BY
    SUM_TIMER_WAIT DESC
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


def collect(db):
    """Collects and prints stats about the given DB instance."""

    ts = now()

    # update master/slave status.
    db.check_set_master()
    db.check_set_slave()

    stmt_by_digest = db.query(
        STMT_BY_DIGEST_QUERY % {
            'last_seen_seconds': MAX_QUERY_LAST_SEEN_SECONDS,
            'limit': NUM_QUERY_SUMMARY_LIMIT,
        }
    )

    # (metric_name, tags) => metric_value
    top_tier_metrics = defaultdict(int)
    bot_tier_metrics = defaultdict(int)

    for row in stmt_by_digest:
        stmt_summary = to_dict(db, row)
        digest = stmt_summary['digest']
        command = get_command_from_digest_text(stmt_summary['digest_text'])

        # create tags
        def create_tags(digest_prefix):
            return " schema_name=%s digest_prefix=%s cmd=%s" % (
                stmt_summary['schema_name'], digest_prefix, command
            )

        # accumulate metrics for different tiers
        for metric_name in METRICS:
            # top tier
            metric_value = stmt_summary[metric_name]
            name = '%s.%s' % (metric_name, digest[:1])
            tags = create_tags(digest[:2])
            top_tier_metrics[(name, tags)] += metric_value

            # bottom tier
            name = '%s.%s' % (metric_name, digest[:2])
            tags = create_tags(digest)
            bot_tier_metrics[(name, tags)] += metric_value

    # report metrics
    for metrics in (top_tier_metrics, bot_tier_metrics):
        for (name, tags), value in metrics.iteritems():
            print_metric(db, ts, "perf_schema.stmt_by_digest.%s" % name, value, tags)


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(collect_loop(collect, COLLECTION_INTERVAL, sys.argv))
