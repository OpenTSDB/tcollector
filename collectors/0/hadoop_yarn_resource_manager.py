#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2010  The tcollector Authors.
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

"""

TCollector plugin to get metrics from Hadoop Yarn Resource Manager JMX API

"""

import os
import sys
import time

try:
  import json
except ImportError:
  json = None
import argparse

SRCDIR = os.path.join(os.path.dirname(__file__))
LIBDIR = os.path.join(SRCDIR, '..', 'lib')
sys.path.append(LIBDIR)
# pylint: disable=wrong-import-position
from collectors.lib import utils
from collectors.lib.hadoop_http import HadoopHttp

REPLACEMENTS = {
}


class HadoopYarnResourceManager(HadoopHttp):
  """
  Class that will retrieve metrics from an Apache Hadoop Yarn Resource Manager JMX API

  Tested on Apache Hadoop 2.7
  """

  def __init__(self, host='localhost', port=8088):
    super(HadoopYarnResourceManager, self).__init__('hadoop',
                                                    'yarn.resource_manager',
                                                    host,
                                                    port)

  def emit(self):
    current_time = int(time.time())
    metrics = self.poll()
    for context, metric_name, value in metrics:
      for key, value in REPLACEMENTS.items():
        if any(_.startswith(key) for _ in context):
          context = value
      self.emit_metric(context, current_time, metric_name, value)


# args are useful for testing but no given by TCollector so will inherit defaults normally
def main(args):
  """ Calls HadoopYarnResourceManager at interval secs
      and emits metrics to stdout for TCollector """
  if json is None:
    utils.err("This collector requires the `json' Python module.")
    return 13  # Ask tcollector not to respawn us
  utils.drop_privileges()
  parser = argparse.ArgumentParser()
  parser.add_argument('-H', '--host', default='localhost',
                      help='Host to connect to (default: localhost)')
  parser.add_argument('-P', '--port', default=8088, type=int,
                      help='Port to connect to (default: 8088)')
  parser.add_argument('-i', '--interval', default=90, type=int,
                      help='Interval at which to emit metrics')
  args = parser.parse_args(args[1:])
  host = args.host
  port = args.port
  interval = args.interval
  yarn_service = HadoopYarnResourceManager(host=host, port=port)
  while True:
    yarn_service.emit()
    time.sleep(interval)
  return 0


if __name__ == "__main__":
  sys.exit(main(sys.argv))
