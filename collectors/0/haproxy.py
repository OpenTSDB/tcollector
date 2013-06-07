#!/usr/bin/env python

import os
import re
import socket
import sys
import time
import subprocess
from subprocess import Popen, PIPE

COLLECTION_INTERVAL = 15

class Proxy:
  def __init__(self):
    self.conf = self._find_conf_file()
    self.sock = self._find_sock_file()

  def _find_conf_file(self):

    """Returns the conf file of haproxy."""
    ptree = subprocess.Popen(["pgrep", "-lf", "haproxy.cfg"], stdout=PIPE).stdout.read()
    conf_file = ptree.split('-f')[1].split()[0]
    return conf_file

  def _find_sock_file(self):

    """Returns the unix socket file of haproxy."""
    conf_file = self._find_conf_file()
    fd = open(conf_file,'r')
    for line in fd:
      if 'stats socket' in line:
        sock_file = line.split()[2]
    return sock_file

  def collect_stats(self):
 
    """Collects stats from haproxy unix domain socket"""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(self.sock)
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
  p = Proxy()
  while True:
    p.collect_stats()
    time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
  main()
