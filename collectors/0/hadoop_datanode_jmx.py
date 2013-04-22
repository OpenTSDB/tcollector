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
import subprocess
import sys
import time

# If this user doesn't exist, we'll exit immediately.
# If we're running as root, we'll drop privileges using this user.
USER = "hadoop"

# We add those files to the classpath if they exist.
CLASSPATH = [
    "/usr/lib/jvm/java-6-sun/lib/tools.jar",
]

# Map certain JVM stats so they are unique and shorter
JMX_SERVICE_RENAMING = {
  "GarbageCollector": "datanode.gc",
  "OperatingSystem": "datanode.os",
  "Threading": "datanode.threads",
}

IGNORED_METRICS = set(["revision", "hdfsUser", "hdfsDate", "hdfsUrl", "date",
                       "hdfsRevision", "user", "hdfsVersion", "url", "version",
                       "NamenodeAddress", "Version", "RpcPort", "HttpPort",
                       # These are useless as-is because they represent the
                       # thread that's dedicated to serving JMX RPCs.
                       "CurrentThreadCpuTime", "CurrentThreadUserTime",
                       # List of directories used by the DataNode.
                       "StorageInfo",
                       "VolumeInfo",
                      ])

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


def main(argv):
    utils.drop_privileges(user=USER)
    # Build the classpath.
    dir = os.path.dirname(sys.argv[0])
    jar = os.path.normpath(dir + "/../lib/jmx-1.0.jar")
    if not os.path.exists(jar):
        print >>sys.stderr, "WTF?!  Can't run, %s doesn't exist" % jar
        return 13
    classpath = [jar]
    for jar in CLASSPATH:
        if os.path.exists(jar):
            classpath.append(jar)
    classpath = ":".join(classpath)

    jmx = subprocess.Popen(
        ["java", "-enableassertions", "-enablesystemassertions",  # safe++
         "-Xmx64m",  # Low RAM limit, to avoid stealing too much from prod.
         "-cp", classpath, "com.stumbleupon.monitoring.jmx",
         "--watch", "10", "--long", "--timestamp",
         "DataNode",  # Name of the process.
         # The remaining arguments are pairs (mbean_regexp, attr_regexp).
         # The first regexp is used to match one or more MBeans, the 2nd
         # to match one or more attributes of the MBeans matched.
         "hadoop", "",                     # All HBase / hadoop metrics.
         "Threading", "Count|Time$",       # Number of threads and CPU time.
         "OperatingSystem", "OpenFile",    # Number of open files.
         "GarbageCollector", "Collection", # GC runs and time spent GCing.
         ], stdout=subprocess.PIPE, bufsize=1)
    do_on_signal(signal.SIGINT, kill, jmx)
    do_on_signal(signal.SIGPIPE, kill, jmx)
    do_on_signal(signal.SIGTERM, kill, jmx)
    try:
        prev_timestamp = 0
        while True:
            line = jmx.stdout.readline()

            if not line and jmx.poll() is not None:
                break  # Nothing more to read and process exited.
            elif len(line) < 4:
                print >>sys.stderr, "invalid line (too short): %r" % line
                continue

            timestamp, metric, value, mbean = line.split("\t", 3)
            # Sanitize the timestamp.
            try:
                timestamp = int(timestamp)
                if timestamp < time.time() - 600:
                    raise ValueError("timestamp too old: %d" % timestamp)
                if timestamp < prev_timestamp:
                    raise ValueError("timestamp out of order: prev=%d, new=%d"
                                     % (prev_timestamp, timestamp))
            except ValueError, e:
                print >>sys.stderr, ("Invalid timestamp on line: %r -- %s"
                                     % (line, e))
                continue
            prev_timestamp = timestamp

            if metric in IGNORED_METRICS:
              continue

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
            if mbean_domain not in ("hadoop", "java.lang"):
                print >>sys.stderr, ("Unexpected mbean domain = %r on line %r"
                                     % (mbean_domain, line))
                continue
            mbean_properties = dict(prop.split("=", 1)
                                    for prop in mbean_properties.split(","))
            if mbean_domain == "hadoop":
              # jmx_service is HBase by default, but we can also have
              # RegionServer or Replication and such.
              jmx_service = mbean_properties.get("service", "HBase")
              if jmx_service == "HBase":
                  jmx_service = "regionserver"
            elif mbean_domain == "java.lang":
                jmx_service = mbean_properties.pop("type", "jvm")
                if mbean_properties:
                    tags += " " + " ".join(k + "=" + v for k, v in
                                           mbean_properties.iteritems())
            else:
                assert 0, "Should never be here"

            jmx_service = JMX_SERVICE_RENAMING.get(jmx_service, jmx_service)
            metric = jmx_service.lower() + "." + metric

            sys.stdout.write("hadoop.%s %d %s%s\n"
                             % (metric, timestamp, value, tags))
            sys.stdout.flush()
    finally:
        kill(jmx)
        time.sleep(300)
        return 0  # Ask the tcollector to re-spawn us.


if __name__ == "__main__":
    sys.exit(main(sys.argv))
