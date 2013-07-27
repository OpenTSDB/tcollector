#!/usr/bin/env python

import os
import re
import socket
import sys
import time
import stat
import subprocess
from subprocess import Popen, PIPE
from collectors.lib import utils

COLLECTION_INTERVAL = 15

def err(e):
  print >>sys.stderr, e

def haproxy_pid():
  """Finds out the pid of haproxy process"""
  try:
     pid = subprocess.check_output(["pidof", "haproxy"])
  except subprocess.CalledProcessError:
     return None
  return pid.rstrip()

def find_conf_file(pid):
  """Returns the conf file of haproxy."""
  try:
     output = subprocess.check_output(["ps", "--no-headers", "-o", "cmd", pid])
  except subprocess.CalledProcessError, e:
     err("HAProxy (pid %s) went away? %s" % (pid, e))
     return None
  return output.split("-f")[1].split()[0]

def find_sock_file(conf_file):
  """Returns the unix socket file of haproxy."""
  try:
    fd = open(conf_file)
  except IOError, e:
    err("Error: %s. Config file path is relative: %s" % (e, conf_file))
    return None
  try:
    for line in fd:
      if line.lstrip(" \t").startswith("stats socket"):
        sock_file = line.split()[2]
        if utils.is_sockfile(sock_file):
          return sock_file
  finally:
    fd.close()

def collect_stats(sock_file):
  """Collects stats from haproxy unix domain socket"""
  sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
  sock.connect(sock_file)
  sock.send("show stat\n")
  stats = sock.recv(10240)
  sock.close()
  ts = time.time()
  for line in stats.split("\n"):
    var = line.split(",")
    if var[0]:
      if var[1] in ("svname", "BACKEND", "FRONTEND"):
        continue
      print ("haproxy.current_sessions %i %s server=%s cluster=%s"
             % (ts, var[4], var[1], var[0]))
      print ("haproxy.session_rate %i %s server=%s cluster=%s"
             % (ts, var[33], var[1], var[0]))

def main():
  pid = haproxy_pid()
  if not pid:
    err("Error: HAProxy is not running")
    return 13  # Ask tcollector to not respawn us.

  conf_file = find_conf_file(pid)
  if not conf_file:
    return 13
  sock_file = find_sock_file(conf_file)
  if sock_file is None:
    err("Error: HAProxy is not listening on any unix domain socket")
    return 13

  while True:
    collect_stats(sock_file)
    time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
  sys.exit(main())
