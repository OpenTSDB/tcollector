#!/usr/bin/python
#
# mongo.py -- a MongoDB collector for tcollector/OpenTSDB
# Copyright 2013 Tim Douglas, me@timdoug.com
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

import sys
import time
import pymongo

HOST = 'localhost'
PORT = 27017
INTERVAL = 15
METRICS = (
    'backgroundFlushing.average_ms',
    'backgroundFlushing.flushes',
    'backgroundFlushing.total_ms',
    'connections.available',
    'connections.current',
    'cursors.totalOpen',
    'cursors.timedOut',
    'dur.commits',
    'dur.commitsInWriteLock',
    'dur.compression',
    'dur.earlyCommits',
    'dur.journaledMB',
    'dur.writeToDataFilesMB',
    'extra_info.heap_usage_bytes',
    'extra_info.page_faults',
    'globalLock.lockTime',
    'globalLock.totalTime',
    'indexCounters.btree.accesses',
    'indexCounters.btree.hits',
    'indexCounters.btree.missRatio',
    'indexCounters.btree.misses',
    'indexCounters.btree.resets',
    'mem.resident',
    'mem.virtual',
    'mem.mapped',
    'network.bytesIn',
    'network.bytesOut',
    'network.numRequests',
)
TAG_METRICS = (
    ('asserts',     ('msg', 'regular', 'user', 'warning')),
    ('opcounters',  ('command', 'delete', 'getmore', 'insert', 'query', 'update')),
)

def main():
    c = pymongo.Connection(host=HOST, port=PORT)

    while True:
        res = c.admin.command('serverStatus')
        ts = int(time.time())

        for base_metric, tags in TAG_METRICS:
            for tag in tags:
                print 'mongo.'+base_metric, ts, res[base_metric][tag], 'type=' + tag
        for metric in METRICS:
            cur = res
            try:
                for m in metric.split('.'):
                    cur = cur[m]
            except KeyError:
                continue
            print 'mongo.' + metric, ts, cur

        sys.stdout.flush()
        time.sleep(INTERVAL)

if __name__ == '__main__':
    main()
