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

from collectors.etc import zabbix_bridge_conf
from collectors.lib import utils


def main(args):
    if BinLogStreamReader is None:
        utils.err("error: Python module `pymysqlreplication' is missing")
        return 1
    if pymysql is None:
        utils.err("error: Python module `pymysql' is missing")
        return 1
    mysql_settings = zabbix_bridge_conf.get_mysql_creds()

    # server_id is your slave identifier, it should be unique.
    # set blocking to True if you want to block and wait for the next event at
    # the end of the stream
    stream = BinLogStreamReader(connection_settings=mysql_settings,
                                server_id=3,
                                only_events=[WriteRowsEvent],
                                blocking=True)

    hostmap = gethostmap(mysql_settings) # TODO: consider reloading peridically
    for binlogevent in stream:
        if binlogevent.schema == mysql_settings['db']:
            table = binlogevent.table
            log_pos = binlogevent.packet.log_pos
            if table == 'history' or table == 'history_uint':
                for row in binlogevent.rows:
                    r = row['values']
                    hm = hostmap[r['itemid']]
                    print "zbx.%s %d %s host=%s proxy=%s" % (hm['key'], r['clock'], r['value'], hm['host'], hm['proxy'])
                sys.stdout.flush()

    stream.close()


def gethostmap(mysql_settings):
    conn = pymysql.connect(**mysql_settings)
    cur = conn.cursor()
    cur.execute("SELECT i.itemid, i.key_, h.host, h2.host AS proxy FROM items i JOIN hosts h ON i.hostid=h.hostid LEFT JOIN hosts h2 ON h2.hostid=h.proxy_hostid")
    # Translation of item key_
    # Note: http://opentsdb.net/docs/build/html/user_guide/writing.html#metrics-and-tags
    unallow = re.compile('[^a-zA-Z0-9\-_\.]')
    hostmap = {}
    for row in cur:
        hostmap[row[0]] = { 'key': re.sub(unallow, '_', row[1]), 'host': re.sub(unallow, '_', row[2]), 'proxy': row[3] }
    cur.close()
    conn.close()
    return hostmap

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
