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

import time

try:
    import json
except ImportError:
    json = None

from collectors.lib import utils
from collectors.lib.hadoop_http import HadoopHttp

EMIT_REGION = True

EXCLUDED_CONTEXTS = ("master")

class HBaseRegionserver(HadoopHttp):
    def __init__(self):
        super(HBaseRegionserver, self).__init__("hbase", "regionserver", "localhost", 60030)

    def emit_region_metric(self, context, current_time, full_metric_name, value):
        split_metric = full_metric_name.split("_")
        if len(split_metric) < 7:
            utils.err("Error splitting %s" % full_metric_name)
            return

        namespace = split_metric[1]
        table = split_metric[3]
        region = split_metric[5]
        metric_name = "_".join(split_metric[7:])
        tag_dict = {"namespace": namespace, "table": table, "region": region}

        if any( not v for k,v in tag_dict.iteritems()):
            utils.err("Error splitting %s", full_metric_name)
        else:
            self.emit_metric(context, current_time, metric_name, value, tag_dict)

    def emit(self):
        """
        Emit metrics from a HBase regionserver.

        This will only emit per region metrics is EMIT_REGION is set to true
        """
        current_time = int(time.time())
        metrics = self.poll()
        for context, metric_name, value in metrics:
            if any( c in EXCLUDED_CONTEXTS for c in context):
                continue

            if any(c == "regions" for c in context):
                if EMIT_REGION:
                    self.emit_region_metric(context, current_time, metric_name, value)
            else:
                self.emit_metric(context, current_time, metric_name, value)


def main(args):
    utils.drop_privileges()
    if json is None:
        utils.err("This collector requires the `json' Python module.")
        return 13  # Ask tcollector not to respawn us
    hbase_service = HBaseRegionserver()
    while True:
        hbase_service.emit()
        time.sleep(15)
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main(sys.argv))

