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

def haproxy_pid():

  """Finds out the pid of haproxy process"""
  pid = subprocess.Popen(["pidof", "haproxy"],stdout=PIPE).stdout.read()
  return pid.rstrip()

def find_sock_file(pid):

  """Returns the unix socket file of haproxy."""
  out = subprocess.Popen(["lsof", "-U", "-a", "-p", pid, "-Fn"],stdout=PIPE).stdout.read()
  for line in out.split("\n"):
    if line.startswith("n"):
      # name of socket file printed by lsof looks like "<file>.<pid>.tmp", hence taking out the actual socket filename.
      sock_file = line[::-1].split('.',2)[2][::-1][1::1]
      if utils.is_sockfile(sock_file):
        return sock_file

def collect_stats(sock_file):

  """Collects stats from haproxy unix domain socket"""
  sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
  sock.connect(sock_file)
  sock.send('show stat\n')
  stats = sock.recv(10240)
  sock.close()
  ts = time.time()
  for line in stats.split("\n"):
    var = line.split(',')
    if var[0]:
      if var[1] in ["svname", "BACKEND", "FRONTEND"]:
        continue
      else:
        print ("haproxy.current_sessions %i %s server=%s cluster=%s" % (ts, var[4], var[1], var[0]))
        print ("haproxy.session_rate %i %s server=%s cluster=%s" % (ts, var[33], var[1], var[0]))

def main():

  pid = haproxy_pid()
  if not pid:
    return 13                                     # Ask tcollector not to respawn us.

  sock_file = find_sock_file(pid)
  if sock_file is None:
    return 13                                     # Ask tcollector not to respawn us.
  else:
    while True:
      collect_stats(sock_file)
      time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
  main()
