#!/usr/bin/env python

#
# Dump all replication item/metric insert events from a zabbix mysql server
#

import re
import sys

try:
    from pymysqlreplication import BinLogStreamReader
    from pymysqlreplication.row_event import (
        WriteRowsEvent
    )
except ImportError:
    BinLogStreamReader = None  # This is handled gracefully in main()

try:
    import pymysql
except ImportError:
    pymysql = None # This is handled gracefully in main()

# from collectors.etc import zabbix_bridge_conf
from collectors.lib import utils

MYSQL_SETTINGS = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': '',
    'passwd': '',
    'db': 'zabbix'
}


def main(args):
    if BinLogStreamReader is None:
        utils.err("error: Python module `pymysqlreplication' is missing")
        return 1
    if pymysql is None:
        utils.err("error: Python module `pymysql' is missing")
        return 1

    # server_id is your slave identifier, it should be unique.
    # set blocking to True if you want to block and wait for the next event at
    # the end of the stream
    stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                                server_id=3,
                                only_events=[WriteRowsEvent],
                                blocking=True)

    for binlogevent in stream:
        if binlogevent.schema == MYSQL_SETTINGS['db']:
            table = binlogevent.table
            log_pos = binlogevent.packet.log_pos
            if table == 'history' or table == 'history_uint':
                for row in binlogevent.rows:
                    r = row['values']
                    print "zbx.raw.%s %d %s log_pos=%s table=%s" % (r['itemid'], r['clock'], r['value'], log_pos, table)
                sys.stdout.flush()

    stream.close()


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main(sys.argv))


## Sample zabbix debug dump:
# === WriteRowsEvent ===
# Date: 2014-08-04T03:47:37
# Log position: 249670
# Event size: 135
# Read bytes: 10
# Table: zabbix.history
# Affected columns: 4
# Changed rows: 5
# Values:
# --
# ('*', u'itemid', ':', 23253)
# ('*', u'ns', ':', 14761117)
# ('*', u'value', ':', 0.0)
# ('*', u'clock', ':', 1407124053)
# --
# ('*', u'itemid', ':', 23254)
# ('*', u'ns', ':', 19470979)
# ('*', u'value', ':', 0.0)
# ('*', u'clock', ':', 1407124054)
# --
# ('*', u'itemid', ':', 23255)
# ('*', u'ns', ':', 19872263)
# ('*', u'value', ':', 0.0)
# ('*', u'clock', ':', 1407124055)
# --
# ('*', u'itemid', ':', 23256)
# ('*', u'ns', ':', 20960622)
# ('*', u'value', ':', 0.0)
# ('*', u'clock', ':', 1407124056)
# --
# ('*', u'itemid', ':', 23257)
# ('*', u'ns', ':', 22024251)
# ('*', u'value', ':', 0.0254)
# ('*', u'clock', ':', 1407124057)
# ()

## SQL item and host info
# SELECT i.itemid, i.key_, h.host, h2.host FROM items i
#  JOIN hosts h ON i.hostid=h.hostid
#  LEFT JOIN hosts h2 ON h2.hostid=h.proxy_hostid;

## Translation of item key_
# Note: http://opentsdb.net/docs/build/html/user_guide/writing.html#metrics-and-tags
# sub(/[^a-zA-Z0-9\-_\.]/,"_")
