#!/usr/bin/python
# This file is part of tcollector.
# Copyright (C) 2011  StumbleUpon, Inc.
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
# Tested with ES 0.18.x, 0.19.x

import errno
import httplib
try:
  import json
except ImportError:
  json = None  # Handled gracefully in main.  Not available by default in <2.6
import socket
import sys
import time

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
  return request(server, "/_cluster/nodes/_local/stats?all=true")

def version(server):
  v = request(server, "/")["version"]["number"].encode("ascii","ignore").split(".")
  major = v[0]
  minor = v[1]
  rev = v[2]
  return major,minor,rev

def main(argv):
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
  major, minor, rev = version(server)
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
    printmetric("indices.store", indices["store"]["size_in_bytes"])
    printmetric("num_docs", indices["docs"]["count"])
    printmetric("num_docs_deleted", indices["docs"]["deleted"])
    d = indices["indexing"]
    printmetric("indexing.total", d["index_total"])
    printmetric("indexing.time", d["index_time_in_millis"] / 1000.)
    printmetric("indexing.current", d["index_current"])
    printmetric("delete.total", d["delete_total"])
    printmetric("delete.time", d["delete_time_in_millis"] / 1000.)
    printmetric("delete.current", d["delete_current"])
    d = indices["get"]
    printmetric("get.current", d["current"])
    printmetric("get.total", d["total"])
    printmetric("get.time", d["time_in_millis"] / 1000.)
    printmetric("exists.total", d["exists_total"])
    printmetric("exists.time", d["exists_time_in_millis"] / 1000.)
    printmetric("missing.total", d["missing_total"])
    printmetric("missing.time", d["missing_time_in_millis"] / 1000.)
    d = indices["search"]
    printmetric("query.total", d["query_total"])
    printmetric("query.time", d["query_time_in_millis"] / 1000.)
    printmetric("query.current", d["query_current"])
    printmetric("fetch.total", d["fetch_total"])
    printmetric("fetch.time", d["fetch_time_in_millis"] / 1000.)
    printmetric("fetch.current", d["fetch_current"])
    d = indices["cache"]
    printmetric("cache.field.evictions", d["field_evictions"])
    printmetric("cache.field.size", d["field_size_in_bytes"])
    printmetric("cache.filter.count", d["filter_count"])
    printmetric("cache.filter.evictions", d["filter_evictions"])
    printmetric("cache.filter.size", d["filter_size_in_bytes"])
    d = indices["merges"]
    printmetric("merges.current", d["current"])
    printmetric("merges.current.docs", d["current_docs"])
    printmetric("merges.current.size", d["current_size_in_bytes"])
    printmetric("merges.total", d["total"])
    printmetric("merges.total_time", d["total_time_in_millis"] / 1000.)
    printmetric("merges.total.docs", d["total_docs"])
    printmetric("merges.total.size", d["total_size_in_bytes"])
    d = indices["refresh"]
    printmetric("refresh.total", d["total"])
    printmetric("refresh.total_time", d["total_time_in_millis"] / 1000.)
    d = indices["flush"]
    printmetric("flush.total", d["total"])
    printmetric("flush.total_time", d["total_time_in_millis"] / 1000.)
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
    if int(minor) >= 17:
      for stat, value in nstats.get("http", {}).iteritems():
        if is_numeric(value):
          printmetric("http." + stat, value)
    # New in ES 0.19:
    if int(minor) >= 19:
      jvm = nstats["jvm"]
      for gc, d in jvm["mem"]["pools"].iteritems():
        gc = gc.encode("ascii","ignore").replace(" ","_")
        printmetric("jvm.mem.pools", d["used_in_bytes"], gc=gc, type="used")
        printmetric("jvm.mem.pools", d["max_in_bytes"], gc=gc, type="max")
        printmetric("jvm.mem.pools", d["peak_max_in_bytes"], gc=gc, type="peak")
      t = nstats["thread_pool"]
      for p, d in t.iteritems():
        printmetric("thread.pool.threads", d["threads"], pool=p)
        printmetric("thread.pool.queue", d["queue"], pool=p)
        printmetric("thread.pool.active", d["active"], pool=p)
      fs = nstats["fs"]["data"]
      for id, data in enumerate(fs):
        mount = data["mount"]
        dev = data["dev"].encode("ascii","ignore").split("/")[-1]
        printmetric("data.total", data["total_in_bytes"], mount=mount, device=dev)
        printmetric("data.free", data["free_in_bytes"], mount=mount, device=dev)
        printmetric("data.available", data["available_in_bytes"], mount=mount, device=dev)
        printmetric("data.reads", data["disk_reads"], mount=mount, device=dev)
        printmetric("data.read.size", data["disk_read_size_in_bytes"], mount=mount, device=dev)
        printmetric("data.writes", data["disk_writes"], mount=mount, device=dev)
        printmetric("data.write.size", data["disk_write_size_in_bytes"], mount=mount, device=dev)
        printmetric("data.disk.queue", data["disk_queue"], mount=mount, device=dev)
        printmetric("data.disk.service_time", data["disk_service_time"], mount=mount, device=dev)
      del jvm
      del t
      del fs
    del nstats
    time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
  sys.exit(main(sys.argv))
