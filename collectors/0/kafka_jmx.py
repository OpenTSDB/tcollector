#!/usr/bin/python

import os
import re
import signal
import subprocess
import sys
import time

from collectors.lib import utils

# How oftent to poll
INTERVAL="60"

# If this user doesn't exist, we'll exit immediately.
# If we're running as root, we'll drop privileges using this user.
USER = "kafka_user"

# We add those files to the classpath if they exist.
CLASSPATH = [
    "/usr/lib/jvm/default-java/lib/tools.jar",
]

# Map certain JVM stats so they are unique and shorter
JMX_SERVICE_RENAMING = {
  "GarbageCollector": "gc",
  "OperatingSystem": "os",
  "Threading": "threads",
}

IGNORED_METRICS = frozenset(["Loggers", "MBeanName"])

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

    jpid = "Kafka"
    jps = subprocess.check_output("jps").split("\n")
    for item in jps:
      vals = item.split(" ")
      if len(vals) == 2:
        if vals[1] == "Kafka":
          jpid = vals[0]
          break
    jmx = subprocess.Popen(
        ["java", "-enableassertions", "-enablesystemassertions",  # safe++
         "-Xmx64m",  # Low RAM limit, to avoid stealing too much from prod.
         "-cp", classpath, "com.stumbleupon.monitoring.jmx",
         "--watch", INTERVAL, "--long", "--timestamp",
         jpid,  # Name of the process.
         # The remaining arguments are pairs (mbean_regexp, attr_regexp).
         # The first regexp is used to match one or more MBeans, the 2nd
         # to match one or more attributes of the MBeans matched.
         "kafka", "",                     # All HBase / hadoop metrics.
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
            mbean_domain = mbean_domain.rstrip().replace("\"", "");
            mbean_properties = mbean_properties.rstrip().replace("\"", "");

            if mbean_domain not in ("kafka.server", "kafka.cluster", "kafka.controller", "kafka.network", "kafka.log", "kafka.consumer", "java.lang"):
                print >>sys.stderr, ("Unexpected mbean domain = %r on line %r"
                                     % (mbean_domain, line))
                continue

            mbean_properties = dict(prop.split("=", 1)
                                    for prop in mbean_properties.split(","))

            if mbean_domain == "kafka.cluster":
                jmx_service = mbean_properties.get("type", "cluster");
                if mbean_properties:
                    tags += " " + " ".join(k + "=" + v for k, v in
                                           mbean_properties.iteritems())
            elif mbean_domain == "kafka.controller":
                jmx_service = mbean_properties.get("type", "controller");
                if mbean_properties:
                    tags += " " + " ".join(k + "=" + v for k, v in
                                           mbean_properties.iteritems())
            elif mbean_domain == "kafka.network":
                jmx_service = mbean_properties.get("type", "network");
                if mbean_properties:
                    tags += " " + " ".join(k + "=" + v for k, v in
                                           mbean_properties.iteritems())
            elif mbean_domain == "kafka.server":
                jmx_service = mbean_properties.get("type", "server");
                if mbean_properties:
                    tags += " " + " ".join(k + "=" + v for k, v in
                                           mbean_properties.iteritems())
            elif mbean_domain == "kafka.log":
                jmx_service = mbean_properties.get("type", "log");
                if mbean_properties:
                    tags += " " + " ".join(k + "=" + v for k, v in
                                           mbean_properties.iteritems())
            elif mbean_domain == "kafka.consumer":
                jmx_service = mbean_properties.pop("type", "consumer")
                if mbean_properties:
                    tags += " " + " ".join(k + "=" + v for k, v in
                                           mbean_properties.iteritems())
            elif mbean_domain == "java.lang":
                jmx_service = mbean_properties.pop("type", "jvm")
                if mbean_properties:
                    tags += " " + " ".join(k + "=" + v for k, v in
                                           mbean_properties.iteritems())
            else:
                assert 0, "Should never be here"

            jmx_service = JMX_SERVICE_RENAMING.get(jmx_service, jmx_service)
            metric = mbean_domain + "." + jmx_service.lower() + "." + metric

            sys.stdout.write("%s %d %s%s\n"
                             % (metric, timestamp, value, tags))
            sys.stdout.flush()
    finally:
        kill(jmx)
        time.sleep(300)
        return 0  # Ask the tcollector to re-spawn us.

if __name__ == "__main__":
    sys.exit(main(sys.argv))