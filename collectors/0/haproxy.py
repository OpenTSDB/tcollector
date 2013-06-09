#!/usr/bin/env python

import os
import re
import socket
import sys
import time
import stat
import subprocess
from subprocess import Popen, PIPE

COLLECTION_INTERVAL = 15

def haproxy_pid():
  
  """Finds out the pid of haproxy process""" 
  pid = subprocess.Popen(["pidof", "haproxy"],stdout=PIPE).stdout.read()
  return pid.rstrip()

def find_conf_file(pid):

  """Returns the conf file of haproxy."""
  ptree = subprocess.Popen(["ps", "-eo", "pid,cmd"], stdout=PIPE).stdout.read()
  for line in ptree.split('\n'):
    if len(line) > 1:
      if line.split()[0] == pid:
        conf_file = line.split("-f")[1].split()[0]
        return conf_file

def is_sock_file(sock_file):
  
  """Checks whether the file is a socket file or not."""
  try:
    s = os.stat(sock_file)
  except OSError, (no, e):
    if no == errno.ENOENT:
      return False
    err("Warning: Couldn't stat(%r): %s" % (sock_file, e))
    return None
  return s.st_mode & stat.S_IFSOCK == stat.S_IFSOCK

def find_sock_file(conf_file):

  """Returns the unix socket file of haproxy."""
  fd = open(conf_file,'r')
  for line in fd:
    if line.lstrip(' \t').startswith('stats socket'):
      sock_file = line.split()[2]  
      if is_sock_file(sock_file):
        return sock_file
  return 13

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
    return 13                                     # Ask tcollector to not respawn us.
  
  conf_file = find_conf_file(pid)
  sock_file = find_sock_file(conf_file)
  while True:
    collect_stats(sock_file)
    time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
  main()
