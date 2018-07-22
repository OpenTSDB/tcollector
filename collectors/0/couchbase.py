#!/usr/bin/env python

"""
Couchbase collector

Refer to the following cbstats documentation for more details:

http://docs.couchbase.com/couchbase-manual-2.1/#cbstats-tool
"""

import os
import sys
import time
import subprocess
import re

from collectors.etc import couchbase_conf
from collectors.lib import utils

CONFIG = couchbase_conf.get_config()
COLLECTION_INTERVAL = CONFIG['collection_interval']
COUCHBASE_INITFILE = CONFIG['couchbase_initfile']

KEYS = frozenset( [
                  'bucket_active_conns',
                  'cas_hits',
                  'cas_misses',
                  'cmd_get',
                  'cmd_set',
                  'curr_connections',
                  'curr_conns_on_port_11209',
                  'curr_conns_on_port_11210',
                  'ep_queue_size',
                  'ep_num_value_ejects',
                  'ep_num_eject_failures',
                  'ep_oom_errors',
                  'ep_tmp_oom_errors',
                  'get_hits',
                  'get_misses',
                  'mem_used',
                  'total_connections',
                  'total_heap_bytes',
                  'total_free_bytes',
                  'total_allocated_bytes',
                  'total_fragmentation_bytes',
                  'tcmalloc_current_thread_cache_bytes',
                  'tcmalloc_max_thread_cache_bytes',
                  'tcmalloc_unmapped_bytes',
                  ] )

def find_couchbase_pid():
  """Find out the pid of couchbase"""
  if not os.path.isfile(COUCHBASE_INITFILE):
    return

  try:
    fd = open(COUCHBASE_INITFILE)
    for line in fd:
      if line.startswith("exec"):
        init_script = line.split()[1]
    fd.close()
  except IOError:
    utils.err("Check permission of file (%s)" % COUCHBASE_INITFILE)
    return

  try:
    fd = open(init_script)
    for line in fd:
      if line.startswith("PIDFILE"):
        pid_file = line.split("=")[1].rsplit()[0]
    fd.close()
  except IOError:
    utils.err("Check permission of file (%s)" % init_script)
    return

  try:
    fd = open(pid_file)
    pid = fd.read()
    fd.close()
  except IOError:
    utils.err("Couchbase-server is not running, since no pid file exists")
    return

  return pid.split()[0]

def find_conf_file(pid):
  """Returns config file for couchbase-server."""
  try:
    fd = open('/proc/%s/cmdline' % pid)
  except IOError as e:
    utils.err("Couchbase (pid %s) went away ? %s" % (pid, e))
    return
  try:
    config = fd.read().split("config_path")[1].split("\"")[1]
    return config
  finally:
    fd.close()

def find_bindir_path(config_file):
  """Returns the bin directory path"""
  try:
    fd = open(config_file)
  except IOError as e:
    utils.err("Error for Config file (%s): %s" % (config_file, e))
    return None
  try:
    for line in fd:
      if line.startswith("{path_config_bindir"):
        return line.split(",")[1].split("\"")[1]
  finally:
    fd.close()

def list_bucket(bin_dir):
  """Returns the list of memcached or membase buckets"""
  buckets = []
  if not os.path.isfile("%s/couchbase-cli" % bin_dir):
    return buckets
  cli = ("%s/couchbase-cli" % bin_dir)
  try:
    buck = subprocess.check_output([cli, "bucket-list", "--cluster",
                                    "localhost:8091"])
  except subprocess.CalledProcessError:
    return buckets
  regex = re.compile("[\s\w]+:[\s\w]+$")
  for i in buck.splitlines():
    if not regex.match(i):
      buckets.append(i)
  return buckets

def collect_stats(bin_dir, bucket):
  """Returns statistics related to a particular bucket"""
  if not os.path.isfile("%s/cbstats" % bin_dir):
    return
  cli = ("%s/cbstats" % bin_dir)
  try:
    ts = time.time()
    stats = subprocess.check_output([cli, "localhost:11211", "-b", bucket,
                                     "all"])
  except subprocess.CalledProcessError:
    return
  for stat in stats.splitlines():
    metric = stat.split(":")[0].lstrip(" ")
    value = stat.split(":")[1].lstrip(" \t")
    if metric in KEYS:
      print("couchbase.%s %i %s bucket=%s" % (metric, ts, value, bucket))

def main():
  utils.drop_privileges()
  pid = find_couchbase_pid()
  if not pid:
    utils.err("Error: Either couchbase-server is not running or file (%s)"
        " doesn't exist" % COUCHBASE_INITFILE)
    return 13

  conf_file = find_conf_file(pid)
  if not conf_file:
    utils.err("Error: Can't find config file (%s)" % conf_file)
    return 13

  bin_dir = find_bindir_path(conf_file)
  if not bin_dir:
    utils.err("Error: Can't find bindir path in config file")
    return 13

  while True:
    # Listing bucket everytime so as to start collecting datapoints
    # of any new bucket.
    buckets = list_bucket(bin_dir)
    for b in buckets:
      collect_stats(bin_dir, b)
    time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
  sys.exit(main())
