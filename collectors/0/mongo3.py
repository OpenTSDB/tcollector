#!/usr/bin/env python
#
# This file is part of tcollector.
# mongo3.py -- a MongoDB 3.x collector for tcollector/OpenTSDB
# Copyright (C) 2016  The tcollector Authors.
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

from __future__ import print_function

import sys
import time
import os
try:
    import pymongo
except ImportError:
    pymongo = None  # This is handled gracefully in main()

from collectors.lib import utils
from collectors.etc import mongodb3_conf

DB_NAMES = []
CONFIG_CONN = []
MONGOS_CONN = []
REPLICA_CONN = []

USER = ''
PASS = ''
INTERVAL = 15

CONFIG_METRICS = (
    'asserts.msg',
    'asserts.regular',
    'asserts.rollovers',
    'asserts.user',
    'asserts.warning',
    'backgroundFlushing.average_ms',
    'backgroundFlushing.flushes',
    'backgroundFlushing.total_ms',
    'connections.available',
    'connections.current',
    'cursors.clientCursors_size',
    'cursors.pinned',
    'cursors.timedOut',
    'cursors.totalNoTimeout',
    'cursors.totalOpen',
    'dur.commits',
    'dur.commitsInWriteLock',
    'dur.compression',
    'dur.earlyCommits',
    'dur.journaledMB',
    'dur.timeMs.commits',
    'dur.timeMs.commitsInWriteLock',
    'dur.timeMs.dt',
    'dur.timeMs.prepLogBuffer',
    'dur.timeMs.remapPrivateView',
    'dur.timeMs.writeToDataFiles',
    'dur.timeMs.writeToJournal',
    'dur.writeToDataFilesMB',
    'extra_info.heap_usage_bytes',
    'extra_info.page_faults',
    'globalLock.activeClients.readers',
    'globalLock.activeClients.total',
    'globalLock.activeClients.writers',
    'globalLock.currentQueue.readers',
    'globalLock.currentQueue.total',
    'globalLock.currentQueue.writers',
    'globalLock.lockTime',
    'globalLock.totalTime',
    'indexCounters.btree.accesses',
    'indexCounters.btree.hits',
    'indexCounters.btree.missRatio',
    'indexCounters.btree.misses',
    'indexCounters.btree.resets',
    'mem.mapped',
    'mem.mappedWithJournal',
    'mem.resident',
    'mem.virtual',
    'metrics.cursor.open.multiTarget',
    'metrics.cursor.open.noTimeout',
    'metrics.cursor.open.pinned',
    'metrics.cursor.open.singleTarget',
    'metrics.cursor.open.total',
    'metrics.cursor.timedOut',
    'metrics.document.deleted',
    'metrics.document.inserted',
    'metrics.document.returned',
    'metrics.document.updated',
    'metrics.getLastError.wtime.num',
    'metrics.getLastError.wtime.totalMillis',
    'metrics.getLastError.wtimeouts',
    'metrics.operation.fastmod',
    'metrics.operation.idhack',
    'metrics.operation.scanAndOrder',
    'metrics.queryExecutor.scanned',
    'metrics.record.moves',
    'metrics.repl.apply.batches.num',
    'metrics.repl.apply.batches.totalMillis',
    'metrics.repl.apply.ops',
    'metrics.repl.buffer.count',
    'metrics.repl.buffer.maxSizeBytes',
    'metrics.repl.buffer.sizeBytes',
    'metrics.repl.network.bytes',
    'metrics.repl.network.getmores.num',
    'metrics.repl.network.getmores.totalMillis',
    'metrics.repl.network.ops',
    'metrics.repl.network.readersCreated',
    'metrics.repl.oplog.insert.num',
    'metrics.repl.oplog.insert.totalMillis',
    'metrics.repl.oplog.insertBytes',
    'metrics.repl.preload.docs.num',
    'metrics.repl.preload.docs.totalMillis',
    'metrics.repl.preload.indexes.num',
    'metrics.repl.preload.indexes.totalMillis',
    'metrics.ttl.deletedDocuments',
    'metrics.ttl.passes',
    'network.bytesIn',
    'network.bytesOut',
    'network.numRequests',
    'opcounters.command',
    'opcounters.delete',
    'opcounters.getmore',
    'opcounters.insert',
    'opcounters.query',
    'opcounters.update',
    'opcountersRepl.command',
    'opcountersRepl.delete',
    'opcountersRepl.getmore',
    'opcountersRepl.insert',
    'opcountersRepl.query',
    'opcountersRepl.update',
    'storage.freelist.search.bucketExhausted',
    'storage.freelist.search.requests',
    'storage.freelist.search.scanned',
)

CONFIG_LOCKS_METRICS = (
    'locks.Collection.acquireCount',
    'locks.Database.acquireCount',
    'locks.Global.acquireCount',
    'locks.MMAPV1Journal.acquireCount',
    'locks.MMAPV1Journal.acquireWaitCount',
    'locks.MMAPV1Journal.timeAcquiringMicros',
    'locks.Metadata.acquireCount'
)

MONGOS_METRICS = (
    'objects',
    'avgObjSize',
    'dataSize',
    'storageSize',
    'numExtents',
    'indexes',
    'indexSize',
    'fileSize',
    'extentFreeList.num',
    'extentFreeList.totalSize'
)

MONGOS_RAW_METRICS = (
    'collections',
    'objects',
    'avgObjSize',
    'dataSize',
    'storageSize',
    'numExtents',
    'indexes',
    'indexSize',
    'fileSize'
)

REPLICA_METRICS = (
    'pingMs',
    'uptime'
)

def runServerStatus(c):
    res = c.admin.command('serverStatus')
    ts = int(time.time())

    for metric in CONFIG_METRICS:
        cur = res
        try:
            for m in metric.split('.'):
                cur = cur[m]
        except KeyError:
            continue
        print('mongo.%s %d %s' % (metric, ts, cur))

    for metric in CONFIG_LOCKS_METRICS:
        cur = res
        try:
            for m in metric.split('.'):
                cur = cur[m]
        except KeyError:
            continue
        for k, v in cur.items():
            print('mongo.%s %d %s mode=%s' % (metric, ts, v, k))

def runDbStats(c):
    for db_name in DB_NAMES:
        res = c[db_name].command('dbStats')
        ts = int(time.time())

        for metric in MONGOS_METRICS:
            cur = res
            try:
                for m in metric.split('.'):
                    cur = cur[m]
            except KeyError:
                continue
            print('mongo.db.%s %d %s db=%s' % (metric, ts, cur, db_name))

        raw_metrics = res['raw']
        for key, value in raw_metrics.items():
            replica_name = key.split('/', 1)[0]
            replica_desc = key.split('/', 1)[1]

            for metric in MONGOS_RAW_METRICS:
                cur = value
                try:
                    for m in metric.split('.'):
                        cur = cur[m]
                except KeyError:
                    continue
                print('mongo.rs.%s %d %s replica=%s db=%s' % (metric, ts, cur, replica_name, db_name))

def runReplSetGetStatus(c):
    res = c.admin.command('replSetGetStatus')
    ts = int(time.time())

    replica_set_name = res['set']
    rs_status = res['myState']
    rs_members = res['members']

    for replica in res['members']:
        replica_name = replica['name'].replace(':', '_')
        replica_state = replica['stateStr']
        if int(replica['health']) == 1:
            replica_health = 'online'
        else:
            replica_health = 'offline'

        for metric in REPLICA_METRICS:
            cur = replica
            try:
                for m in metric.split('.'):
                    cur = cur[m]
            except KeyError:
                continue
            print('mongo.replica.%s %d %s replica_set=%s replica=%s replica_state=%s replica_health=%s' % (metric, ts, cur, replica_set_name, replica_name, replica_state, replica_health))

def loadEnv():
    global USER, PASS, INTERVAL, DB_NAMES, CONFIG_CONN, MONGOS_CONN, REPLICA_CONN
    for item in mongodb3_conf.get_settings()['db'].split(','):
        DB_NAMES.append(item)

    for item in mongodb3_conf.get_settings()['config'].split(','):
        if item:
            host_port = item.split(':')
            CONFIG_CONN.append({'host': host_port[0], 'port': int(host_port[1]), 'link': None})

    for item in mongodb3_conf.get_settings()['mongos'].split(','):
        if item:
            host_port = item.split(':')
            MONGOS_CONN.append({'host': host_port[0], 'port': int(host_port[1]), 'link': None})

    for item in mongodb3_conf.get_settings()['replica'].split(','):
        if item:
            host_port = item.split(':')
            REPLICA_CONN.append({'host': host_port[0], 'port': int(host_port[1]), 'link': None})
    USER = mongodb3_conf.get_settings()['username']
    PASS = mongodb3_conf.get_settings()['password']
    INTERVAL = mongodb3_conf.get_settings()['interval']

def main():
    loadEnv()

    utils.drop_privileges()
    if pymongo is None:
        print("error: Python module `pymongo' is missing", file=sys.stderr)
        return 13

    for index, item in enumerate(CONFIG_CONN, start=0):
        conn = pymongo.MongoClient(host=item['host'], port=item['port'])
        if USER:
            conn.admin.authenticate(USER, PASS, mechanism='DEFAULT')
        CONFIG_CONN[index]['link'] = conn

    for index, item in enumerate(MONGOS_CONN, start=0):
        conn = pymongo.MongoClient(host=item['host'], port=item['port'])
        if USER:
            conn.admin.authenticate(USER, PASS, mechanism='DEFAULT')
        MONGOS_CONN[index]['link'] = conn

    for index, item in enumerate(REPLICA_CONN, start=0):
        conn = pymongo.MongoClient(host=item['host'], port=item['port'])
        if USER:
            conn.admin.authenticate(USER, PASS, mechanism='DEFAULT')
        REPLICA_CONN[index]['link'] = conn

    while True:
        for conn in CONFIG_CONN:
            runServerStatus(conn['link'])

        for conn in MONGOS_CONN:
            runDbStats(conn['link'])

        for conn in REPLICA_CONN:
            runReplSetGetStatus(conn['link'])

        sys.stdout.flush()
        time.sleep(INTERVAL)

if __name__ == '__main__':
    sys.exit(main())
