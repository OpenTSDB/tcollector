#!/usr/bin/python
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
"""ElasticSearch collector"""  # Because ES is cool, bonsai cool.
# Tested with ES 0.16.5 and 0.17.x

import errno
import httplib
try:
  import json
except ImportError:
  json = None  # Handled gracefully in main.  Not available by default in <2.6
import socket
import sys
import time

from collectors.lib import utils


COLLECTION_INTERVAL = 15  # seconds
DEFAULT_TIMEOUT = 10.0    # seconds
ES_HOST = "localhost"
ES_PORT = 9200  # TCP port on which ES listens.

STATUS_MAP = {
  "green": 0,
  "yellow": 1,
  "red": 2,
}


def is_numeric(value):
  return isinstance(value, (int, long, float))


def err(msg):
  print >>sys.stderr, msg


class ESError(RuntimeError):
  """Exception raised if we don't get a 200 OK from ElasticSearch."""

  def __init__(self, resp):
    RuntimeError.__init__(self, str(resp))
    self.resp = resp


def request(server, uri):
  """Does a GET request of the given uri on the given HTTPConnection."""
  server.request("GET", uri)
  resp = server.getresponse()
  if resp.status != httplib.OK:
    raise ESError(resp)
  return json.loads(resp.read())


def cluster_health(server):
  return request(server, "/_cluster/health")


def cluster_state(server):
  return request(server, "/_cluster/state"
                 + "?filter_routing_table=true&filter_metadata=true&filter_blocks=true")


def node_stats(server):
  return request(server, "/_cluster/nodes/_local/stats")


def main(argv):
  utils.drop_privileges()
  socket.setdefaulttimeout(DEFAULT_TIMEOUT)
  server = httplib.HTTPConnection(ES_HOST, ES_PORT)
  try:
    server.connect()
  except socket.error, (erno, e):
    if erno == errno.ECONNREFUSED:
      return 13  # No ES running, ask tcollector to not respawn us.
    raise
  if json is None:
    err("This collector requires the `json' Python module.")
    return 1

  nstats = node_stats(server)
  cluster_name = nstats["cluster_name"]
  nodeid, nstats = nstats["nodes"].popitem()

  ts = None
  def printmetric(metric, value, **tags):
    if tags:
      tags = " " + " ".join("%s=%s" % (name, value)
                            for name, value in tags.iteritems())
    else:
      tags = ""
    print ("elasticsearch.%s %d %s cluster=%s%s"
           % (metric, ts, value, cluster_name, tags))

  while True:
    ts = int(time.time())
    nstats = node_stats(server)
    # Check that the node's identity hasn't changed in the mean time.
    if nstats["cluster_name"] != cluster_name:
      err("cluster_name changed from %r to %r"
          % (cluster_name, nstats["cluster_name"]))
      return 1
    this_nodeid, nstats = nstats["nodes"].popitem()
    if this_nodeid != nodeid:
      err("node ID changed from %r to %r" % (nodeid, this_nodeid))
      return 1

    is_master = nodeid == cluster_state(server)["master_node"]
    printmetric("is_master", int(is_master))
    if is_master:
      ts = int(time.time())  # In case last call took a while.
      cstats = cluster_health(server)
      for stat, value in cstats.iteritems():
        if stat == "status":
          value = STATUS_MAP.get(value, -1)
        elif not is_numeric(value):
          continue
        printmetric("cluster." + stat, value)

    ts = nstats["os"]["timestamp"] / 1000  # ms -> s
    indices = nstats["indices"]
    printmetric("indices.size", indices["size_in_bytes"])
    printmetric("num_docs", indices["docs"]["num_docs"])
    d = indices["cache"]
    printmetric("cache.field.evictions", d["field_evictions"])
    printmetric("cache.field.size", d["field_size_in_bytes"])
    printmetric("cache.filter.count", d["filter_count"])
    printmetric("cache.filter.evictions", d["filter_evictions"])
    printmetric("cache.filter.size", d["filter_size_in_bytes"])
    d = indices["merges"]
    printmetric("merges.current", d["current"])
    printmetric("merges.total", d["total"])
    printmetric("merges.total_time", d["total_time_in_millis"] / 1000.)
    del indices
    process = nstats["process"]
    ts = process["timestamp"] / 1000  # ms -> s
    open_fds = process.get("open_file_descriptors")  # ES 0.17
    if open_fds is None:
      open_fds = process.get("fd")  # ES 0.16
      if open_fds is not None:
        open_fds = open_fds["total"]
    if open_fds is not None:
      printmetric("process.open_file_descriptors", open_fds)
    d = process["cpu"]
    printmetric("process.cpu.percent", d["percent"])
    printmetric("process.cpu.sys", d["sys_in_millis"] / 1000.)
    printmetric("process.cpu.user", d["user_in_millis"] / 1000.)
    d = process["mem"]
    printmetric("process.mem.resident", d["resident_in_bytes"])
    printmetric("process.mem.shared", d["share_in_bytes"])
    printmetric("process.mem.total_virtual", d["total_virtual_in_bytes"])
    del process
    jvm = nstats["jvm"]
    ts = jvm["timestamp"] / 1000  # ms -> s
    d = jvm["mem"]
    printmetric("jvm.mem.heap_used", d["heap_used_in_bytes"])
    printmetric("jvm.mem.heap_committed", d["heap_committed_in_bytes"])
    printmetric("jvm.mem.non_heap_used", d["non_heap_used_in_bytes"])
    printmetric("jvm.mem.non_heap_committed", d["non_heap_committed_in_bytes"])
    d = jvm["threads"]
    printmetric("jvm.threads.count", d["count"])
    printmetric("jvm.threads.peak_count", d["peak_count"])
    for gc, d in jvm["gc"]["collectors"].iteritems():
      printmetric("jvm.gc.collection_count", d["collection_count"], gc=gc)
      printmetric("jvm.gc.collection_time",
                  d["collection_time_in_millis"] / 1000., gc=gc)
    del jvm
    del d
    for stat, value in nstats["network"]["tcp"].iteritems():
      if is_numeric(value):
        printmetric("network.tcp." + stat, value)
    for stat, value in nstats["transport"].iteritems():
      if is_numeric(value):
        printmetric("transport." + stat, value)
    # New in ES 0.17:
    for stat, value in nstats.get("http", {}).iteritems():
      if is_numeric(value):
        printmetric("http." + stat, value)
    del nstats
    time.sleep(COLLECTION_INTERVAL)


if __name__ == "__main__":
  sys.exit(main(sys.argv))
