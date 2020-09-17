#!/usr/bin/python
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

import sys
import time

try:
    import json
except ImportError:
    json = None

from collectors.lib import utils
from collectors.lib.hadoop_http import HadoopHttp


REPLACEMENTS = {
    "rpcdetailedactivityforport": ["rpc_activity"],
    "rpcactivityforport": ["rpc_activity"]
}


class HadoopJournalNode(HadoopHttp):
    """
    Class that will retrieve metrics from an Apache Hadoop JournalNode's jmx page.

    This requires Apache Hadoop 1.0+ or Hadoop 2.0+.
    Anything that has the jmx page will work but the best results will com from Hadoop 2.1.0+
    """

    def __init__(self):
        super(HadoopJournalNode, self).__init__('hadoop', 'journalnode', 'localhost', 8480)

    def emit(self):
        current_time = int(time.time())
        metrics = self.poll()
        for context, metric_name, value in metrics:
            for k, v in REPLACEMENTS.items():
                if any(c.startswith(k) for c in context):
                    context = v
            self.emit_metric(context, current_time, metric_name, value)


def main(args):
    utils.drop_privileges()
    if json is None:
        utils.err("This collector requires the `json' Python module.")
        return 13  # Ask tcollector not to respawn us
    journalnode_service = HadoopJournalNode()
    while True:
        journalnode_service.emit()
        time.sleep(90)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

