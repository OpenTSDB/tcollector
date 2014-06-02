#!/usr/bin/env python

def get_interval():
  """How frequent to scan G1 GC log (in seconds) """
  return 10

def get_gc_config():
  """
  prefix: the full metric name will be prefix.gc.g1.*
  log_dir: GC log director, e.g. /var/logs/g1gc/
  log_name_pattern: python glob pattern that will be used to find latest gc log
  """
  return {'prefix': 'jvm', 'log_dir': '/var/log/gc', 'log_name_pattern': '*gc*.log'}
