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
#  to a local sqlite cache (that can also be shared).
#

import os
import re
import sqlite3
import sys
import time
try:
    import pymysql
except ImportError:
    pymysql = None # This is handled gracefully in main()

from collectors.etc import zabbix_bridge_conf
from collectors.lib import utils


def main():
    utils.drop_privileges()
    if pymysql is None:
        utils.err("error: Python module `pymysql' is missing")
        return 1
    settings = zabbix_bridge_conf.get_settings()

    db_filename = settings['sqlitedb']
    db_is_new = not os.path.exists(db_filename)
    dbcache = sqlite3.connect(db_filename)

    if db_is_new:
        utils.err("Zabbix bridge SQLite DB file does not exist; creating: %s" % (db_filename))
        cachecur = dbcache.cursor()
        cachecur.execute('''CREATE TABLE zabbix_cache
             (id integer, key text, host text, proxy text)''')
        dbcache.commit()
    else:
        utils.err("Zabbix bridge SQLite DB exists @ %s" % (db_filename))


    dbzbx = pymysql.connect(**settings['mysql'])
    zbxcur = dbzbx.cursor()
    zbxcur.execute("SELECT i.itemid, i.key_, h.host, h2.host AS proxy FROM items i JOIN hosts h ON i.hostid=h.hostid LEFT JOIN hosts h2 ON h2.hostid=h.proxy_hostid")
    # Translation of item key_
    # Note: http://opentsdb.net/docs/build/html/user_guide/writing.html#metrics-and-tags
    disallow = re.compile(settings['disallow'])
    cachecur = dbcache.cursor()
    print('tcollector.zabbix_bridge.deleterows %d %s' %
     (int(time.time()), cachecur.execute('DELETE FROM zabbix_cache').rowcount))
    rowcount = 0
    for row in zbxcur:
        cachecur.execute('''INSERT INTO zabbix_cache(id, key, host, proxy) VALUES (?,?,?,?)''',
         (row[0], re.sub(disallow, '_', row[1]), re.sub(disallow, '_', row[2]), row[3]))
        rowcount += 1

    print('tcollector.zabbix_bridge.rows %d %s' % (int(time.time()), rowcount))
    zbxcur.close()
    dbcache.commit()

    dbzbx.close()
    dbcache.close()


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main())
