#!/usr/bin/python

"""Common utility functions for interacting with Java processes."""

import os
import subprocess

JAVA_HOME = os.getenv('JAVA_HOME', '/usr/lib/jvm/default-java')
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
                      ])

__java_args = [JAVA, "-enableassertions", "-enablesystemassertions",  # safe++
        "-Xmx64m",  # Low RAM limit, to avoid stealing too much from prod.
        "-cp", ":".join(CLASSPATH), "com.stumbleupon.monitoring.jmx",
        "--watch", str(PERIOD), "--long", "--timestamp"]

def init_jmx_process(jvm_name, *watched_mbeans):
    """Start a process that watches the given JVM and periodically print out
    values of watched beans.

    Arguments:
      jvm_name: name of the Java process to connect to 
      watched_mbeans: pairs of mbean and mbean attributes regexps to filter java metrics

    Returns: Popen for running process
    """
    cmd_args = __java_args + [jvm_name] + list(watched_mbeans)
    return subprocess.Popen(cmd_args, stdout=subprocess.PIPE, bufsize=1)
