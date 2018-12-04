#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2015  The tcollector Authors.
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

def enabled():
  return False

def get_config():
  """Configuration for the Docker collector

    On EL6 distros (CentOS/RHEL/Scientific/OL) the cgroup path should be:
      "/cgroup"
  """
  import platform
  # Scientific Linux says 'redhat' here
  # CentOS 5 says 'redhat'
  # CentOS >=6 says 'centos'
  # CentOS >=7 cgroup is located on /sys/fs/cgroup
  if platform.dist()[0] in ['centos', 'redhat'] and not platform.dist()[1].startswith("7."):
    cgroup_path = '/cgroup'
  else:
    cgroup_path = '/sys/fs/cgroup'

  config = {
    'interval': 15,
    'socket_path': '/var/run/docker.sock',
    'cgroup_path': cgroup_path
  }

  return config
