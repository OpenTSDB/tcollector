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

try:
    import json
    import time
except ImportError:
    json = None

from Queue import Queue
from collectors.lib import utils
from collectors.lib.hadoop_http import HadoopHttp
from collectors.lib.hadoop_http import HadoopNode
from collectors.lib.collectorbase import CollectorBase


REPLACEMENTS = {
    "datanodeactivity-": ["activity"],
    "fsdatasetstate-ds-": ["fs_data_set_state"],
    "rpcdetailedactivityforport": ["rpc_activity"],
    "rpcactivityforport": ["rpc_activity"]
}


class HadoopDataNode(CollectorBase):
    def __init__(self, config, logger, readq):
        super(HadoopDataNode, self).__init__(config, logger, readq)

        self.service = self.get_config('service', 'hadoop')
        self.daemon = self.get_config('daemon', 'datanode')
        self.host = self.get_config('host', 'localhost')
        self.port = self.get_config('port', 50075)
        self.readq = readq

    def __call__(self):
        with utils.lower_privileges(self._logger):
            if json:
                self._readq.nput("hadoop.datanode.state %s %s" % (int(time.time()), '0'))
                HadoopNode(self.service, self.daemon, self.host, self.port, REPLACEMENTS, self.readq, self._logger).emit()
            else:
                self._readq.nput("hadoop.datanode.state %s %s" % (int(time.time()), '1'))
                self.logger.error("This collector requires the `json' Python module.")


if __name__ == "__main__":
    from collectors.lib.utils import TestQueue
    from collectors.lib.utils import TestLogger

    hadoopdatanode_inst = HadoopDataNode(None, TestLogger(), TestQueue())
    hadoopdatanode_inst()
