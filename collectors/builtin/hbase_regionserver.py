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

import time
import re
from collectors.lib.collectorbase import CollectorBase

try:
    import json
except ImportError:
    json = None

from collectors.lib import utils
from collectors.lib.hadoop_http import HadoopHttp

EMIT_REGION = True

EXCLUDED_CONTEXTS = ("master")
REGION_METRIC_PATTERN = re.compile(r"[N|n]amespace_(.*)_table_(.*)_region_(.*)_metric_(.*)")


class HBaseRegionserverHttp(HadoopHttp):
    def __init__(self, host, port, logger, readq):
        super(HBaseRegionserverHttp, self).__init__("hbase", "regionserver", host, port, readq, logger)

    def emit_region_metric(self, context, current_time, full_metric_name, value):
        match = REGION_METRIC_PATTERN.match(full_metric_name)
        if not match:
            self.logger.error("Error splitting %s" % full_metric_name)
            return

        namespace = match.group(1)
        table = match.group(2)
        region = match.group(3)
        metric_name = match.group(4)
        tag_dict = {"namespace": namespace, "table": table, "region": region}

        if any( not v for k,v in tag_dict.iteritems()):
            self.logger.error("Error splitting %s" % full_metric_name)
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


class HbaseRegionserver(CollectorBase):
    def __init__(self, config, logger, readq):
        super(HbaseRegionserver, self).__init__(config, logger, readq)

        self.logger = logger
        self.readq = readq
        self.host = self.get_config('host', 'localhost')
        self.port = self.get_config('port', 16030)

    def __call__(self):
        with utils.lower_privileges(self._logger):
            if json:
                self._readq.nput("hbase.regionserver.state %s %s" % (int(time.time()), '0'))
                HBaseRegionserverHttp(self.host, self.port, self.logger, self.readq).emit()
            else:
                self._readq.nput("hbase.regionserver.state %s %s" % (int(time.time()), '1'))
                self.logger.error("This collector requires the `json' Python module.")


if __name__ == "__main__":
    from collectors.lib.utils import TestQueue
    from collectors.lib.utils import TestLogger

    hbaseregionserver_inst = HbaseRegionserver(None, TestLogger(), TestQueue())
    hbaseregionserver_inst()
