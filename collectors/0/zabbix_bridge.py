#!/usr/bin/env python

#
# Dump all replication item/metric insert events from a zabbix mysql server
#

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent
)

MYSQL_SETTINGS = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': '',
    'passwd': ''
}


def main():
    # server_id is your slave identifier, it should be unique.
    # set blocking to True if you want to block and wait for the next event at
    # the end of the stream
    stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                                server_id=3,
                                only_events=[WriteRowsEvent],
                                blocking=True)

    for binlogevent in stream:
        table = binlogevent.table
        log_pos = binlogevent.packet.log_pos
        if table == 'history' or table == 'history_uint':
            for row in binlogevent.rows:
                r = row['values']
                print "pos:%s table:%s raw.%s %d %s" % (log_pos, table, r['itemid'], r['clock'], r['value'])

    stream.close()


if __name__ == "__main__":
    main()


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
