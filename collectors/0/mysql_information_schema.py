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
"""Collector for MySQL table stats from information_schema.tables"""

import sys

try:
    import MySQLdb
except ImportError:
    MySQLdb = None  # This is handled gracefully in main()

from collectors.lib.mysql_utils import (
    collect_loop,
    now,
    print_metric,
    to_dict,
    find_schemas,
)

COLLECTION_INTERVAL = 60  # seconds

TABLE_SCHEMA_QUERY = """
SELECT
    TABLE_SCHEMA,
    TABLE_NAME,
    ifnull(ENGINE, 'NONE') as ENGINE,
    ifnull(ROW_FORMAT, 'NONE') as ROW_FORMAT,
    ifnull(TABLE_ROWS, '0') as TABLE_ROWS,
    ifnull(AVG_ROW_LENGTH, '0') as AVG_ROW_LENGTH,
    ifnull(DATA_LENGTH, '0') as DATA_LENGTH,
    ifnull(INDEX_LENGTH, '0') as INDEX_LENGTH,
    ifnull(DATA_FREE, '0') as DATA_FREE
FROM
    information_schema.tables
WHERE
    TABLE_SCHEMA = '%s'
"""

TAGS = [
    'table_schema',
    'table_name',
    'engine',
    'row_format',
]

METRICS = [
    'table_rows',
    'avg_row_length',
    'data_length',
    'index_length',
    'data_free',
]

def collect(db):
    """Collects and prints stats about the given DB instance."""

    ts = now()

    # update master/slave status.
    db.check_set_master()
    db.check_set_slave()

    # Per table stats in information_schema.tables are the same across
    # master and slave. Therefore only need to be reported from master.
    if db.is_slave:
        return

    db_list = find_schemas(db)
    for db_name in db_list:
        table_info_list = db.query(TABLE_SCHEMA_QUERY % db_name)
        for row in table_info_list:
            table_info = to_dict(db, row)
            # create tags
            tags = ""
            for tag_name in TAGS:
                if tag_name in table_info:
                    tags = "%s%s" % (tags, " %s=%s" % (tag_name, table_info[tag_name]))
            # output metrics
            for metric_name in METRICS:
                print_metric(
                    db,
                    ts,
                    "info_schema.tables.%s" % metric_name,
                    table_info[metric_name],
                    tags
                )


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(collect_loop(collect, COLLECTION_INTERVAL, sys.argv))
