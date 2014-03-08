#!/usr/bin/python

"""Common utility functions for interacting with Java processes."""

import os
import subprocess
import sys

# use the first directory that exists and appears to be a JDK (not a JRE).

JAVA_HOME = None
TRY_PATHS = [os.getenv('JAVA_HOME'), '/usr/java/latest', '/usr/lib/jvm/default-java']

for path in TRY_PATHS:
    # a JDK will have a lib/tools.jar available
    tools_jar = "%s/lib/tools.jar" % path
    if os.path.isfile(tools_jar):
        JAVA_HOME = path
        break 

if not JAVA_HOME:
    raise Exception("Cannot find a Java installation which looks like a JDK")

JAVA = "%s/bin/java" % JAVA_HOME

CLASSPATH = [
        "%s/lib/tools.jar" % JAVA_HOME,
        os.path.dirname(os.path.realpath(__file__)) + "/../lib/jmx-1.0.jar"
]

PERIOD = 60

IGNORED_METRICS = set(["revision", "hdfsUser", "hdfsDate", "hdfsUrl", "date",
                       "hdfsRevision", "user", "hdfsVersion", "url", "version",
                       "NamenodeAddress", "Version", "RpcPort", "HttpPort",
                       # These are useless as-is because they represent the
                       # thread that's dedicated to serving JMX RPCs.
                       "CurrentThreadCpuTime", "CurrentThreadUserTime",
                       # List of directories used by the DataNode.
                       "StorageInfo",
                       "VolumeInfo",
                       "ServerName",
                       "ZookeeperQuorum",
                       "Verbose",
                       "Type"
                      ])

__java_args = [JAVA, "-enableassertions", "-enablesystemassertions",  # safe++
        "-Xmx64m",  # Low RAM limit, to avoid stealing too much from prod.
        "-cp", ":".join(CLASSPATH), "com.stumbleupon.monitoring.jmx"]

__java_watch_args = ["--watch", str(PERIOD), "--long", "--timestamp"]

def list_procs(jvm_name):
    """Get all pids that match the given jvm_name

    Returns:
      Dictionary that maps pid to process name and arguments
    """
    proc = subprocess.Popen(__java_args + [jvm_name], stdout=subprocess.PIPE, bufsize=1)
    ret = {}
    for line in proc.stdout:
        pid, cmd = line.strip().split('\t')
        ret[pid] = cmd
    return ret

def init_jmx_process(jvm_name, *watched_mbeans):
    """Start a process that watches the given JVM and periodically print out
    values of watched beans.

    Arguments:
      jvm_name: name of the Java process to connect to 
      watched_mbeans: pairs of mbean and mbean attributes regexps to filter java metrics

    Returns: Popen for running process
    """
    cmd_args = __java_args + __java_watch_args + [jvm_name] + list(watched_mbeans)
    print >>sys.stderr, "Starting jmx watch process: %s" % ' '.join(cmd_args)
    return subprocess.Popen(cmd_args, stdout=subprocess.PIPE, bufsize=1)
