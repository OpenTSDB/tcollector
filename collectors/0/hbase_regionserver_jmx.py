#!/usr/bin/python
# This file is part of tcollector.
# Copyright (C) 2010  StumbleUpon, Inc.
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
import pwd
import re
import subprocess
import sys
import time
import traceback

# If this user doesn't exist, we'll exit immediately.
# If we're running as root, we'll drop privileges using this user.
USER = "hadoop"

# We read this file to determine which cluster we're on.
HBASE_CONFIG = "/home/hadoop/hbase/conf/hbase-site.xml"

# We add those files to the classpath if they exist.
CLASSPATH = [
    "/usr/lib/jvm/java-6-sun/lib/tools.jar",
]

def drop_privileges():
    try:
        ent = pwd.getpwnam(USER)
    except KeyError:
        print >>sys.stderr, "Not running, user '%s' doesn't exist" % USER
        sys.exit(13)

    if os.getuid() != 0:
        return

    os.setgid(ent.pw_gid)
    os.setuid(ent.pw_uid)

def main(argv):
    drop_privileges()
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

    # Load the HBase site config.
    if not os.path.exists(HBASE_CONFIG):
        print >>sys.stderr, ("WTF?!  HBase site config (%s) doesn't exist"
                             % HBASE_CONFIG)
        return 13
    sitecfg = dict(re.findall("<name>([^<]+)</name>[^<]*<value>([^<]+)</value>",
                              open(HBASE_CONFIG).read(), re.S))
    cluster = sitecfg.get("zookeeper.znode.parent", "/hbase")[1:]
    if cluster == "hbase":
        if "sv2" in sitecfg.get("hbase.rootdir", ""):  # XXX Don't hardcode sv2
            cluster = "dev"
        else:
            cluster = "prod"

    jmx = subprocess.Popen(
        ["java", "-enableassertions", "-enablesystemassertions",  # safe++
         "-Xmx64m",  # Low RAM limit, to avoid stealing too much from prod.
         "-cp", classpath, "com.stumbleupon.monitoring.jmx",
         "--watch", "10", "--long", "--timestamp",
         "HRegionServer",  # Name of the process.
         "hadoop"],        # regexp for MBeans to retrieve.
         stdout=subprocess.PIPE, bufsize=1)
    try:
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
            except ValueError, e:
                print >>sys.stderr, ("Invalid timestamp on line: %r -- %s"
                                     % (line, e))
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
            mbean_domain, mbean_properties = mbean.split(":", 1)
            if mbean_domain != "hadoop":
                print >>sys.stderr, ("Unexpected mbean domain = %r on line %r"
                                     % (mbean_domain, line))
                continue
            mbean_properties = dict(prop.split("=", 1)
                                    for prop in mbean_properties.split(","))
            # jmx_service is HBase by default, but we can also have
            # RegionServer or Replication and such.
            jmx_service = mbean_properties.get("service", "HBase")
            if jmx_service == "HBase":
                jmx_service = "regionserver"
            else:
                jmx_service, repl_count = re.subn("[^a-zA-Z0-9]+", ".", jmx_service)
                if repl_count:
                    print >>sys.stderr, ("Warning: found malformed"
                                         " jmx_service=%r on line=%r"
                                         % (mbean_properties["service"], line))
            metric = jmx_service.lower() + "." + metric

            sys.stdout.write("hbase.%s %d %s cluster=%s%s\n"
                             % (metric, timestamp, value, cluster, tags))
            sys.stdout.flush()
    finally:
        # Clean up after ourselves.
        jmx.stdout.close()
        rv = jmx.poll()
        if rv is None:
            os.kill(jmx.pid, 15)
            rv = jmx.poll()
            if rv is None:
                os.kill(jmx.pid, 9)  # Bang bang!
                rv = jmx.wait()  # This shouldn't block too long.
        print >>sys.stderr, "warning: jmx exited %d" % rv
        if rv == 2:  # No JVM matched.
            return 13  # Tell the tcollector to not re-spawn us.
        return 0  # Ask the tcollector to re-spawn us immediately.


if __name__ == "__main__":
    sys.exit(main(sys.argv))
