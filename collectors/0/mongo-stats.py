#!/usr/bin/python
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
""" mongodb stats for TSDB """
#
# mongo-stats.py
#
# mongo.mem.resident
# mongo.mem.virtual
# mongo.mem.mapped
# mongo.network.bytesIn
# mongo.network.bytesOut
# mongo.network.numRequests
# mongo.opcounters.insert
# mongo.opcounters.query
# mongo.opcounters.update
# mongo.opcounters.delete
# mongo.opcounters.getmore
# mongo.opcounters.command
# mongo.connections.current
# mongo.connections.available
# mongo.extra_info.heap_usage_bytes
# mongo.extra_info.page_faults
# mongo.asserts.regular
# mongo.asserts.warning
# mongo.asserts.msg
# mongo.asserts.user
# mongo.asserts.rollovers
# mongo.indexCounters.btree.missRatio
# mongo.indexCounters.btree.resets
# mongo.indexCounters.btree.hits
# mongo.indexCounters.btree.misses
# mongo.indexCounters.btree.accesses
# mongo.globalLock.totaltime
# mongo.globalLock.locktime
# mongo.globalLock.ratio
# mongo.globalLock.currentQueue.total
# mongo.globalLock.currentQueue.readers
# mongo.globalLock.currentQueue.writers
# mongo.globalLock.activeClients.total
# mongo.globalLock.activeClients.readers
# mongo.globalLock.activeClients.writers

# REFER: http://www.mongodb.org/display/DOCS/serverStatus+Command

# NOTES:
# * All metrics are tagged with dbhost=
# * You can use mkmetric to create required db schema, for e.g. $ tsdb mkmetric mongo.mem.virtual
# * Make sure that mongo process is started with "--rest" flag
# * Change MONGO_HOST value according to your setup


import urllib
import json
import time
import sys


COLLECTION_INTERVAL = 60 # seconds

# mongo
MONGO_PORT = 28017 # web interface
MONGO_HOST = 'localhost' # CHANGE ME


def fetch_info(host, port):
    """ connect to mongo and fetch server status """
    try:
        m_info = urllib.urlopen('http://' + host + ':' + str(port) + '/serverStatus').read()
        j_info = json.loads(m_info)
        return j_info
    except:
        print 'Could not connect to mongo, please check if port and hostname is right.'


def print_stat(metric, value, tags=""):
    """ prints values in hbase schema format """
    ts = int(time.time())
    if value is not None:
        print "mongo.%s %d %s %s" % (metric, ts, value, tags)


def dispatch_value(info, identifier, metric, extra_info=None):
    ts = int(time.time())
    if extra_info:
        return print_stat(identifier + '.' + metric + '.' + extra_info,
                          info[identifier][metric][extra_info],
                          'dbhost=' + MONGO_HOST)

    return print_stat(identifier + "." + metric,
                      info[identifier][metric],
                      'dbhost=' + MONGO_HOST)


def main():
    """ mongo-stats main loop """

    while True:

    # connect to instance and gather info
        info = fetch_info(MONGO_HOST, MONGO_PORT)
        dispatch_value(info, 'mem', 'resident')
        dispatch_value(info, 'mem', 'virtual')
        dispatch_value(info, 'mem', 'mapped')

        dispatch_value(info, 'network', 'bytesIn')
        dispatch_value(info, 'network', 'bytesOut')
        dispatch_value(info, 'network', 'numRequests')

        dispatch_value(info, 'opcounters', 'insert')
        dispatch_value(info, 'opcounters', 'query')
        dispatch_value(info, 'opcounters', 'update')
        dispatch_value(info, 'opcounters', 'delete')
        dispatch_value(info, 'opcounters', 'getmore')
        dispatch_value(info, 'opcounters', 'command')

        dispatch_value(info, 'connections', 'current')
        dispatch_value(info, 'connections', 'available')

        dispatch_value(info, 'extra_info', 'heap_usage_bytes')
        dispatch_value(info, 'extra_info', 'page_faults')

        dispatch_value(info, 'asserts', 'regular')
        dispatch_value(info, 'asserts', 'warning')
        dispatch_value(info, 'asserts', 'msg')
        dispatch_value(info, 'asserts', 'user')
        dispatch_value(info, 'asserts', 'rollovers')

        dispatch_value(info, 'indexCounters', 'btree', 'missRatio')
        dispatch_value(info, 'indexCounters', 'btree', 'resets')
        dispatch_value(info, 'indexCounters', 'btree', 'hits')
        dispatch_value(info, 'indexCounters', 'btree', 'misses')
        dispatch_value(info, 'indexCounters', 'btree', 'accesses')

        dispatch_value(info, 'globalLock', 'totalTime')
        dispatch_value(info, 'globalLock', 'lockTime')
        dispatch_value(info, 'globalLock', 'ratio')
        dispatch_value(info, 'globalLock', 'currentQueue', 'total')
        dispatch_value(info, 'globalLock', 'currentQueue', 'readers')
        dispatch_value(info, 'globalLock', 'currentQueue', 'writers')
        dispatch_value(info, 'globalLock', 'activeClients', 'total')
        dispatch_value(info, 'globalLock', 'activeClients', 'readers')
        dispatch_value(info, 'globalLock', 'activeClients', 'writers')

        sys.stdout.flush()

        time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
    sys.exit(main())
