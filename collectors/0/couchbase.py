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

COLLECTION_INTERVAL = 15

KEYS = [ 'bucket_active_conns', 
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
	 'tcmalloc_unmapped_bytes' ]

def err(e):
  print >>sys.stderr, e

def find_couchbase_pid():
  """Find out the pid of couchbase"""
  try:
    pid = subprocess.check_output(["pidof", "beam.smp"])
  except subprocess.CalledProcessError:
    return None
  return pid.rstrip()

def find_conf_file(pid):
  """Returns config file for beam.smp process"""
  try:
    fd = open('/proc/%s/cmdline' % pid)
  except IOError, e:
    err("Couchbase (pid %s) went away ? %s" % (pid, e)) 
    return None
  try:
    config = fd.read().split("config_path")[1].split("\"")[1]
    return config	
  finally:
    fd.close()

def find_bindir_path(config_file):
  """Returns the bin directory path"""
  try:
    fd = open(config_file)
  except IOError, e:
    err("Error for Config file (%s): %s" % (config_file, e))
    return None
  try:
    for line in fd:
      if line.startswith("{path_config_bindir"):
        return line.split(",")[1].split("\"")[1]
  finally:
    fd.close()

def list_bucket(couchbase_bindir):
  """Returns the list of memcached or membase buckets"""
  buckets = []
  for d in couchbase_bindir:
    if os.path.isfile("%s/couchbase-cli" % d):
      cli = ("%s/couchbase-cli" % d)
      break
  try:
    buck = subprocess.check_output([cli, "bucket-list", "--cluster", "localhost:8091"])
  except subprocess.CalledProcessError:
    return None
  buck = iter(buck.splitlines())
  regex = re.compile("[\s\w]+:[\s\w]+$")
  for i in buck:
    if not regex.match(i):
      buckets.append(i)
  return buckets	

def collect_stats(couchbase_bindir, bucket):
  """Returns statistics related to a particular bucket"""
  for d in couchbase_bindir:
    if os.path.isfile("%s/cbstats" % d):
      cli = ("%s/cbstats" % d)
      break
  try:
    ts = time.time()
    stats = subprocess.check_output([cli, "localhost:11211", "-b", bucket, "all"])
  except subprocess.CalledProcessError:
    return None
  for stat in stats.splitlines():
    metric = stat.split(":")[0].lstrip(" ")
    value = stat.split(":")[1].lstrip(" \t")
    if metric in KEYS:
      print ("couchbase.%s %i %s bucket=%s" % (metric, ts, value, bucket))

def main():
  config_file = []
  couchbase_bindir = []
  pids = find_couchbase_pid()
  if not pids:
    err("Error: Couchbase is not running")
    return 13
  pids = pids.split()

  for i in pids:
    cfile = find_conf_file(i)
    if cfile is not None and cfile not in config_file:
      config_file.append(cfile)
  if not config_file:
    err("Error: Can't find config file")
    return 13

  for f in config_file:
    bdpath = find_bindir_path(f)
    if bdpath is not None and bdpath not in couchbase_bindir:
      couchbase_bindir.append(bdpath)
  if not couchbase_bindir:
    err("Error: Can't find bindir path in config file")
    return 13
	
  while True:
    buckets = list_bucket(couchbase_bindir)
    for b in buckets:
      collect_stats(couchbase_bindir, b)
    time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
  sys.exit(main())
