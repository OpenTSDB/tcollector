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

import os
import re
import signal
import sys
import time
import threading
import traceback

from collectors.lib import java
from collectors.lib import utils

# If this user doesn't exist, we'll exit immediately.
# If we're running as root, we'll drop privileges using this user.
USER = "optimizely"
NUMBERED_PATTERN = re.compile(r'^(.*-)(\d+)$')

# Map certain JVM stats so they are unique and shorter
JMX_SERVICE_RENAMING = {
  "GarbageCollector": "gc",
  "OperatingSystem": "os",
  "Threading": "threads",
}


def kill(proc):
    """Kills the subprocess given in argument."""
    # Clean up after ourselves.
    rv = proc.poll()
    if rv is None:
        os.kill(proc.pid, signal.SIGTERM)
        rv = proc.poll()
        if rv is None:
            os.kill(proc.pid, signal.SIGKILL)  # Bang bang!
            rv = proc.wait()  # This shouldn't block too long.
    proc.stdout.close()
    utils.err("warning: proc exited %d" % rv)
    return rv


def do_on_signal(signum, func, *args, **kwargs):
    """Calls func(*args, **kwargs) before exiting when receiving signum."""
    def signal_shutdown(signum, frame):
        utils.err("got signal %d, exiting" % signum)
        func(*args, **kwargs)
        sys.exit(128 + signum)
    signal.signal(signum, signal_shutdown)


def get_flume_version(flume_cmd):
    """Parse flume command to find flume version.
    """
    # Flume paths are formatted like so
    # org.apache.flume.node.Application -n agent -f /opt/backend/versions/212/resources/flume/flume.conf
    # we ant to extract 212 from the above line
    flume_path = flume_cmd.split(' ')[-1]
    flume_path_parts = flume_path.split('/')
    # the index of the version number is 1 + the index of the 'versions' directory
    version_index = flume_path_parts.index('versions') + 1
    return flume_path_parts[version_index]


class FlumeJmxMonitor(threading.Thread):
    _REPORT_LOCK = threading.Lock()

    def __init__(self, pid, cmd):
        threading.Thread.__init__(self)

        self.daemon = True
        self.version = get_flume_version(cmd)
        self._prev_timestamp = 0
        self._is_shutdown = False
        self._jmx = java.init_jmx_process(str(pid),
                "com.optimizely", "",
                "org.apache.flume.channel", "",
                "org.apache.flume.sink", "",
                "org.apache.flume.sink", "",
                "org.apache.flume.source", "",
                "Threading", "Count|Time$",       # Number of threads and CPU time.
                "OperatingSystem", "OpenFile",    # Number of open files.
                "GarbageCollector", "Collection", # GC runs and time spent GCing.
                )

        utils.err("Monitoring version: %s Cmd: %s" % (self.version, cmd))

    def run(self):
        while self._jmx.poll() is None:
            line = self._jmx.stdout.readline()
            if len(line) < 4:
                utils.err("invalid line (too short): %r" % line)
                continue

            self._process_line(line)

        utils.err("Stopping monitoring version: %s" % self.version)

    def kill_jmx(self):
        kill(self._jmx)

    def _process_line(self, line):
        try:
            timestamp, metric, value, mbean = line.split("\t", 3)
        except ValueError, e:
            # Temporary workaround for jmx.jar not printing these lines we
            # don't care about anyway properly.
            if "java.lang.String" not in line:
                utils.err("Can't split line: %r" % line)
                return

        if metric in java.IGNORED_METRICS:
            return

        # Sanitize the timestamp.
        try:
            timestamp = int(timestamp)
            if timestamp < time.time() - 600:
                raise ValueError("timestamp too old: %d" % timestamp)
            if timestamp < self._prev_timestamp:
                raise ValueError("timestamp out of order: prev=%d, new=%d"
                        % (self._prev_timestamp, timestamp))
        except ValueError, e:
            utils.err("Invalid timestamp on line: %r -- %s"
                    % (line, e))
            return

        self._prev_timestamp = timestamp

        tags = ""
        # The JMX metrics have per-request-type metrics like so:
        #   metricNameNumOps
        #   metricNameMinTime
        #   metricNameMaxTime
        #   metricNameAvgTime
        # Group related metrics together in the same metric name, use tags
        # to separate the different request types, so we end up with:
        #   numOps op=metricName
        #   avgTime op=metricName
        # etc, which makes it easier to graph things with the TSD.
        if metric.endswith("MinTime"):  # We don't care about the minimum
            return                      # time taken by operations.
        elif metric.endswith("NumOps"):
            tags = " op=" + metric[:-6]
            metric = "numOps"
        elif metric.endswith("AvgTime"):
            tags = " op=" + metric[:-7]
            metric = "avgTime"
        elif metric.endswith("MaxTime"):
            tags = " op=" + metric[:-7]
            metric = "maxTime"

        # mbean is of the form "domain:key=value,...,foo=bar"
        # some tags can have spaces, so we need to fix that.
        mbean_domain, mbean_properties = mbean.rstrip().replace(" ", "_").split(":", 1)
        mbean_properties = dict(prop.split("=", 1) for prop in mbean_properties.split(","))
        if mbean_domain == "java.lang":
            jmx_service = mbean_properties.pop("type", "jvm")
            if mbean_properties:
                tags += " " + " ".join(k + "=" + v for k, v in
                        mbean_properties.iteritems())
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

        jmx_service = JMX_SERVICE_RENAMING.get(jmx_service, jmx_service)
        metric = jmx_service.lower() + "." + metric
        tags += " version=" + str(self.version)

        self._report(metric, timestamp, value, tags)

    def _report(self, metric, timestamp, value, tags):
        with FlumeJmxMonitor._REPORT_LOCK:
            sys.stdout.write("flume.%s %d %s%s\n"
                    % (metric, timestamp, value, tags))
            sys.stdout.flush()


def main(argv):
    utils.drop_privileges(user=USER)

    flume_monitors = {}

    def kill_monitors(join=False):
        for monitor in flume_monitors.values():
            monitor.kill_jmx()
            if join:
                monitor.join()

    do_on_signal(signal.SIGINT, kill_monitors, join=True)
    do_on_signal(signal.SIGPIPE, kill_monitors, join=True)
    do_on_signal(signal.SIGTERM, kill_monitors, join=True)

    try:
        procs = {}

        while True:
            for pid, monitor in flume_monitors.items():
                if not monitor.is_alive():
                    monitor.kill_jmx()
                    del flume_monitors[pid]

            for pid, cmd in procs.iteritems():
                if pid not in flume_monitors:
                    flume_monitors[pid] = FlumeJmxMonitor(pid, cmd)
                    flume_monitors[pid].start()

            # HACK
            # jmx monitor seems not to emit results if attached too early, so
            # we wait one interval before we start monitoring
            procs = java.list_procs("org.apache.flume.node.Application")

            time.sleep(60)
    except Exception as e:
        utils.err('Caught exception: %s' % e)
        traceback.print_exc(file=sys.stderr)
        kill_monitors(join=False)
        time.sleep(60)
        return 0  # Ask the tcollector to re-spawn us.


if __name__ == "__main__":
    sys.exit(main(sys.argv))
