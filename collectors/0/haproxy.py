#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2013 The tcollector Authors.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
# General Public License for more details. You should have received a copy
# of the GNU Lesser General Public License along with this program. If not,
# see <http://www.gnu.org/licenses/>.

# Script uses UNIX socket opened by haproxy, you need to setup one with
# "stats socket" config parameter.
#
# You need to ensure that "stats timeout" (socket timeout) is big
# enough to work well with collector COLLECTION_INTERVAL constant.
# The default timeout on the "stats socket" is set to 10 seconds!
#
# See haproxy documentation for details:
# http://haproxy.1wt.eu/download/1.4/doc/configuration.txt
# section 3.1. Process management and security.

"""HAproxy collector """

import os
import socket
import sys
import time
import stat
import subprocess
from collectors.lib import utils

COLLECTION_INTERVAL = 15

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
     utils.err("HAProxy (pid %s) went away? %s" % (pid, e))
     return None
  return output.split("-f")[1].split()[0]

def find_sock_file(conf_file):
  """Returns the unix socket file of haproxy."""
  try:
    fd = open(conf_file)
  except IOError, e:
    utils.err("Error: %s. Config file path is relative: %s" % (e, conf_file))
    return None
  try:
    for line in fd:
      if line.lstrip(" \t").startswith("stats socket"):
        sock_file = line.split()[2]
        if utils.is_sockfile(sock_file):
          return sock_file
  finally:
    fd.close()

def collect_stats(sock):
  """Collects stats from haproxy unix domain socket"""
  sock.send("show stat\n")
  stats = sock.recv(10240)

  ts = time.time()
  for line in stats.split("\n"):
    var = line.split(",")
    if var[0]:
      # skip ready for next command value "> "
      if var[0] == "> ":
        continue
      if var[1] in ("svname", "BACKEND", "FRONTEND"):
        continue
      print ("haproxy.current_sessions %i %s server=%s cluster=%s"
             % (ts, var[4], var[1], var[0]))
      print ("haproxy.session_rate %i %s server=%s cluster=%s"
             % (ts, var[33], var[1], var[0]))

def main():
  pid = haproxy_pid()
  if not pid:
    utils.err("Error: HAProxy is not running")
    return 13  # Ask tcollector to not respawn us.

  conf_file = find_conf_file(pid)
  if not conf_file:
    return 13

  sock_file = find_sock_file(conf_file)
  if sock_file is None:
    utils.err("Error: HAProxy is not listening on any unix domain socket")
    return 13

  sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
  sock.connect(sock_file)

  # put haproxy to interactive mode, otherwise haproxy closes
  # connection after first command.
  # See haproxy documentation section 9.2. Unix Socket commands.
  sock.send("prompt\n")

  while True:
    collect_stats(sock)
    time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
  sys.exit(main())
