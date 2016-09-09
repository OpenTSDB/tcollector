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
from collectors.lib.collectorbase import CollectorBase
from Queue import Queue

try:
    import json
except ImportError:
    json = None

from collectors.lib import utils
from collectors.lib.hadoop_http import HadoopHttp


EXCLUDED_CONTEXTS = ('regionserver', 'regions', )


class HBaseMasterHttp(HadoopHttp):
    """
    Class to get metrics from Apache HBase's master

    Require HBase 0.96.0+
    """

    def __init__(self, port, logger, readq):
        super(HBaseMasterHttp, self).__init__('hbase', 'master', 'localhost', port, readq, logger)

    def emit(self):
        current_time = int(time.time())
        metrics = self.poll()
        for context, metric_name, value in metrics:
            if any(c in EXCLUDED_CONTEXTS for c in context):
                continue
            self.emit_metric(context, current_time, metric_name, value)


class HbaseMaster(CollectorBase):
    def __init__(self, config, logger, readq):
        super(HbaseMaster, self).__init__(config, logger, readq)

        self.logger = logger
        self.readq = readq
        self.port = self.get_config('port', 60010)

        utils.drop_privileges()

    def __call__(self):
        if json:
            HBaseMasterHttp(self.port, self.logger, self.readq).emit()
        else:
            self.logger.error("This collector requires the `json' Python module.")


if __name__ == "__main__":
    hbasemaster_inst = HbaseMaster(None, None, Queue())
    hbasemaster_inst()
