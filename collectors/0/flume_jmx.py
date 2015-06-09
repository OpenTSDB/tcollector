#!/usr/bin/python
# This file is part of tcollector.
# Copyright (C) 2012  The tcollector Authors.
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

import re
import sys

from collectors.lib.jmx_monitor import JmxMonitor

NUMBERED_PATTERN = re.compile(r'^(.*-)(\d+)$')
MONITORED_MBEANS = ["com.optimizely", "",
                    "org.apache.flume.channel", "",
                    "org.apache.flume.sink", "",
                    "org.apache.flume.sink", "",
                    "org.apache.flume.source", "",
                    "Threading", "Count|Time$",        # Number of threads and CPU time.
                    "OperatingSystem", "OpenFile",     # Number of open files.
                    "GarbageCollector", "Collection"]  # GC runs and time spent GC-ing.


class FlumeJmxMonitor(JmxMonitor):
    USER = "optimizely"
    PROCESS_NAME = "org.apache.flume.node.Application"

    def __init__(self, pid, cmd):
        super(FlumeJmxMonitor, self).__init__(pid, cmd, MONITORED_MBEANS, self.PROCESS_NAME)
        self.version = self.parse_flume_version(cmd)

    # Override
    def process_metric(self, timestamp, metric, value, mbean):
        metric, tags = self.group_metrics(metric)

        # mbean is of the form "domain:key=value,...,foo=bar"
        # some tags can have spaces, so we need to fix that.
        mbean_domain, mbean_properties = mbean.rstrip().replace(" ", "_").split(":", 1)
        mbean_properties = dict(prop.split("=", 1) for prop in mbean_properties.split(","))

        if mbean_domain == "java.lang":
            jmx_service = mbean_properties.pop("type", "jvm")
            if mbean_properties:
                tags += " " + " ".join(k + "=" + v for k, v in
                                       mbean_properties.iteritems())
        elif mbean_domain == "metrics":
            # ex.: metrics:name=com.optimizely.backend.shadowfax.ApiSink.forwarded
            jmx_service = mbean_properties['name']
        else:
            # for flume we use the mbean domain as the prefix
            # this has the form org.apache.flume.source, we strip out the org.apache.flume. part
            jmx_service = mbean_domain[len("org.apache.flume."):]

            # convert channel/sink/source names that are formatted like 'channel-type-1' into
            # two tags type=channel-type num=1
            # this is useful for combining channels/sources/sinks of the same type
            type_ = mbean_properties['type']
            match = NUMBERED_PATTERN.match(type_)
            if match:
                type_ = match.group(1)[:-1]
                tags += " type=%s num=%s" % (type_, match.group(2))
            else:
                tags += " type=" + type_

        jmx_service = JmxMonitor.SHORT_SERVICE_NAMES.get(jmx_service, jmx_service)
        metric = jmx_service.lower() + "." + metric
        tags += " version=" + str(self.version)

        self.emit(metric, timestamp, value, tags)

    @staticmethod
    def parse_flume_version(flume_cmd):
        """Parse flume command to find flume version."""

        # Flume paths are formatted like so
        # org.apache.flume.node.Application -n agent -f
        # /opt/backend/versions/212/resources/flume/flume.conf
        # we want to extract '212' from the above line
        flume_path = flume_cmd.split(' ')[-1]
        flume_path_parts = flume_path.split('/')
        # the index of the version number is 1 + the index of the 'versions' directory
        version_index = flume_path_parts.index('versions') + 1
        return flume_path_parts[version_index]


def main():
    return FlumeJmxMonitor.start_monitors()

if __name__ == "__main__":
    sys.exit(main())
