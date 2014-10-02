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

import sqlite3
import sys
import time

try:
    from pymysqlreplication import BinLogStreamReader
    from pymysqlreplication.row_event import (
        WriteRowsEvent
    )
except ImportError:
    BinLogStreamReader = None  # This is handled gracefully in main()

from collectors.etc import zabbix_bridge_conf
from collectors.lib import utils


def main():
    utils.drop_privileges()
    if BinLogStreamReader is None:
        utils.err("error: Python module `pymysqlreplication' is missing")
        return 1
    settings = zabbix_bridge_conf.get_settings()

    # Set blocking to True if you want to block and wait for the next event at
    # the end of the stream
    stream = BinLogStreamReader(connection_settings=settings['mysql'],
                                server_id=settings['slaveid'],
                                only_events=[WriteRowsEvent],
                                resume_stream=True,
                                blocking=True)

    db_filename = settings['sqlitedb']
    dbcache = sqlite3.connect(':memory:')
    cachecur = dbcache.cursor()
    cachecur.execute("ATTACH DATABASE '%s' as 'dbfile'" % (db_filename,))
    cachecur.execute('CREATE TABLE zabbix_cache AS SELECT * FROM dbfile.zabbix_cache')
    cachecur.execute('CREATE UNIQUE INDEX uniq_zid on zabbix_cache (id)')

    # tcollector.zabbix_bridge namespace for internal Zabbix bridge metrics.
    log_pos = 0
    key_lookup_miss = 0
    sample_last_ts = int(time.time())
    last_key_lookup_miss = 0

    for binlogevent in stream:
        if binlogevent.schema == settings['mysql']['db']:
            table = binlogevent.table
            log_pos = binlogevent.packet.log_pos
            if table == 'history' or table == 'history_uint':
                for row in binlogevent.rows:
                    r = row['values']
                    itemid = r['itemid']
                    cachecur.execute('SELECT id, key, host, proxy FROM zabbix_cache WHERE id=?', (itemid,))
                    row = cachecur.fetchone()
                    if (row is not None):
                        print "zbx.%s %d %s host=%s proxy=%s" % (row[1], r['clock'], r['value'], row[2], row[3])
                        if ((int(time.time()) - sample_last_ts) > settings['internal_metric_interval']): # Sample internal metrics @ 10s intervals
                            sample_last_ts = int(time.time())
                            print "tcollector.zabbix_bridge.log_pos %d %s" % (sample_last_ts, log_pos)
                            print "tcollector.zabbix_bridge.key_lookup_miss %d %s" % (sample_last_ts, key_lookup_miss)
                            print "tcollector.zabbix_bridge.timestamp_drift %d %s" % (sample_last_ts, (sample_last_ts - r['clock']))
                            if ((key_lookup_miss - last_key_lookup_miss) > settings['dbrefresh']):
                                print "tcollector.zabbix_bridge.key_lookup_miss_reload %d %s" % (sample_last_ts, (key_lookup_miss - last_key_lookup_miss))
                                cachecur.execute('DROP TABLE zabbix_cache')
                                cachecur.execute('CREATE TABLE zabbix_cache AS SELECT * FROM dbfile.zabbix_cache')
                                last_key_lookup_miss = key_lookup_miss
                    else:
                        # TODO: Consider https://wiki.python.org/moin/PythonDecoratorLibrary#Retry
                        utils.err("error: Key lookup miss for %s" % (itemid))
                        key_lookup_miss += 1
                sys.stdout.flush()

    dbcache.close()
    stream.close()


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
