#!/usr/bin/env python
#
# Copyright (C) 2014  The tcollector Authors.
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


def main():
    utils.drop_privileges()
    if BinLogStreamReader is None:
        utils.err("error: Python module `pymysqlreplication' is missing")
        return 1
    if pymysql is None:
        utils.err("error: Python module `pymysql' is missing")
        return 1
    settings = zabbix_bridge_conf.get_settings()

    # Set blocking to True if you want to block and wait for the next event at
    # the end of the stream
    stream = BinLogStreamReader(connection_settings=settings['mysql'],
                                server_id=settings['slaveid'],
                                only_events=[WriteRowsEvent],
                                resume_stream=True,
                                blocking=True)

    hostmap = gethostmap(settings) # Prime initial hostmap
    for binlogevent in stream:
        if binlogevent.schema == settings['mysql']['db']:
            table = binlogevent.table
            log_pos = binlogevent.packet.log_pos
            if table == 'history' or table == 'history_uint':
                for row in binlogevent.rows:
                    r = row['values']
                    itemid = r['itemid']
                    try:
                        hm = hostmap[itemid]
                        print "zbx.%s %d %s host=%s proxy=%s" % (hm['key'], r['clock'], r['value'], hm['host'], hm['proxy'])
                    except KeyError:
                        # TODO: Consider https://wiki.python.org/moin/PythonDecoratorLibrary#Retry
                        hostmap = gethostmap(settings)
                        utils.err("error: Key lookup miss for %s" % (itemid))
                sys.stdout.flush()
                # if n seconds old, reload
                # settings['gethostmap_interval']

    stream.close()


def gethostmap(settings):
    conn = pymysql.connect(**settings['mysql'])
    cur = conn.cursor()
    cur.execute("SELECT i.itemid, i.key_, h.host, h2.host AS proxy FROM items i JOIN hosts h ON i.hostid=h.hostid LEFT JOIN hosts h2 ON h2.hostid=h.proxy_hostid")
    # Translation of item key_
    # Note: http://opentsdb.net/docs/build/html/user_guide/writing.html#metrics-and-tags
    disallow = re.compile(settings['disallow'])
    hostmap = {}
    for row in cur:
        hostmap[row[0]] = { 'key': re.sub(disallow, '_', row[1]), 'host': re.sub(disallow, '_', row[2]), 'proxy': row[3] }
    cur.close()
    conn.close()
    return hostmap

if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main())


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
