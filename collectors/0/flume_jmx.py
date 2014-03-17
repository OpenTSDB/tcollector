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

from collections import defaultdict

from lib import java

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

# How many times, maximum, will we attempt to restart the JMX collector.
# If we reach this limit, we'll exit with an error.
MAX_RESTARTS = 10

TOP = False  # Set to True when we want to terminate.
RETVAL = 0    # Return value set by signal handler.


def kill(proc):
  """Kills the subprocess given in argument."""
  # Clean up after ourselves.
  proc.stdout.close()
  rv = proc.poll()
  if rv is None:
      os.kill(proc.pid, 15)
      rv = proc.poll()
      if rv is None:
          os.kill(proc.pid, 9)  # Bang bang!
          rv = proc.wait()  # This shouldn't block too long.
  print >>sys.stderr, "warning: proc exited %d" % rv
  return rv


def do_on_signal(signum, func, *args, **kwargs):
  """Calls func(*args, **kwargs) before exiting when receiving signum."""
  def signal_shutdown(signum, frame):
    print >>sys.stderr, "got signal %d, exiting" % signum
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
    return int(flume_path_parts[version_index])

def main(argv):
    class UpdateFlumeProcesses(threading.Thread):
        UPDATE_INTERVAL = 60

        def __init__(self, procs, procs_lock):
            threading.Thread.__init__(self)
            self.procs = procs
            self.procs_lock = procs_lock
            self.is_shutdown = False
            self.completed_first_run = threading.Semaphore(0)

        def shutdown(self):
            self.is_shutdown = True

        def run(self):
            while not self.is_shutdown:
                self.procs_lock.acquire()
                self.update_flume_processes()
                self.procs_lock.release()
                self.completed_first_run.release()

                # for faster responsiveness on shutdown
                for i in range(UpdateFlumeProcesses.UPDATE_INTERVAL):
                    if self.is_shutdown:
                        break
                    time.sleep(1)

        def kill(self):
            for jmx in self.procs.values():
                kill(jmx)
            self.shutdown()
            self.join()

        def update_flume_processes(self):
            procs = java.list_procs("org.apache.flume.node.Application")
            for pid, cmd in procs.iteritems():
                version = get_flume_version(cmd)
                if version not in self.procs:
                    jmx = java.init_jmx_process(str(pid),
                            "org.apache.flume.channel", "",
                            "org.apache.flume.sink", "",
                            "org.apache.flume.sink", "",
                            "org.apache.flume.source", "",
                            "Threading", "Count|Time$",       # Number of threads and CPU time.
                            "OperatingSystem", "OpenFile",    # Number of open files.
                            "GarbageCollector", "Collection", # GC runs and time spent GCing.
                            )
                    self.procs[version] = jmx

    jmxs = {}
    jmxs_lock = threading.Lock()
    updater = UpdateFlumeProcesses(jmxs, jmxs_lock)
    do_on_signal(signal.SIGINT, updater.kill)
    do_on_signal(signal.SIGPIPE, updater.kill)
    do_on_signal(signal.SIGTERM, updater.kill)

    updater.start()
    updater.completed_first_run.acquire()

    print >>sys.stderr, "Versions: %s" % jmxs.keys()
    try:
        prev_timestamps = defaultdict(lambda: 0)
        while jmxs:
            jmxs_lock.acquire()
            versions = jmx.items()
            jmxs_lock.release()

            for version, jmx in versions:
                line = jmx.stdout.readline()
                if not line and jmx.poll() is not None:
                    print >>sys.stderr, "removing version: %s" % version
                    jmxs_lock.acquire()
                    del jmxs[version]
                    jmxs_lock.release()
                    continue  # Nothing more to read and process exited.
                elif len(line) < 4:
                    print >>sys.stderr, "invalid line (too short): %r" % line
                    continue

                try:
                    timestamp, metric, value, mbean = line.split("\t", 3)
                except ValueError, e:
                    # Temporary workaround for jmx.jar not printing these lines we
                    # don't care about anyway properly.
                    if "java.lang.String" not in line:
                        print >>sys.stderr, "Can't split line: %r" % line
                    continue

                if metric in java.IGNORED_METRICS:
                  continue

                # Sanitize the timestamp.
                try:
                    timestamp = int(timestamp)
                    if timestamp < time.time() - 600:
                        raise ValueError("timestamp too old: %d" % timestamp)
                    if timestamp < prev_timestamps[version]:
                        raise ValueError("timestamp out of order: prev=%d, new=%d"
                                         % (prev_timestamps[version], timestamp))
                except ValueError, e:
                    print >>sys.stderr, ("Invalid timestamp on line: %r -- %s"
                                         % (line, e))
                    continue
                prev_timestamps[version] = timestamp

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
                    continue                    # time taken by operations.
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
                tags += " version=" + str(version)

                sys.stdout.write("flume.%s %d %s%s\n"
                                 % (metric, timestamp, value, tags))
                sys.stdout.flush()
    finally:
        updater.shutdown()
        updater.join()
        for version, jmx in jmxs:
            kill(jmx)
        time.sleep(300)
        return 0  # Ask the tcollector to re-spawn us.


if __name__ == "__main__":
    sys.exit(main(sys.argv))
