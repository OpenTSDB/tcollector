#!/usr/bin/python
#
# Copyright (C) 2011  The tcollector Authors.
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
#
# Written by Mark Smith <mark@qq.is>.
#

"""Statistics from a Redis instance.

Note: this collector parses your Redis configuration files to determine what cluster
this instance is part of.  If you want the cluster tag to be accurate, please edit
your Redis configuration file and add a comment like this somewhere in the file:

# tcollector.cluster = main

You can name the cluster anything that matches the regex [a-z0-9-_]+.

This collector outputs the following metrics:

 - redis.bgrewriteaof_in_progress
 - redis.bgsave_in_progress
 - redis.blocked_clients
 - redis.changes_since_last_save
 - redis.client_biggest_input_buf
 - redis.client_longest_output_list
 - redis.connected_clients
 - redis.connected_slaves
 - redis.expired_keys
 - redis.evicted_keys
 - redis.hash_max_zipmap_entries
 - redis.hash_max_zipmap_value
 - redis.keyspace_hits
 - redis.keyspace_misses
 - redis.mem_fragmentation_ratio
 - redis.pubsub_channels
 - redis.pubsub_patterns
 - redis.total_commands_processed
 - redis.total_connections_received
 - redis.uptime_in_seconds
 - redis.used_cpu_sys
 - redis.used_cpu_user
 - redis.used_memory
 - redis.used_memory_rss

For more information on these values, see this (not very useful) documentation:

    http://redis.io/commands/info
"""

import re
import subprocess
import sys
import time

try:
    import redis
    has_redis = True
except ImportError:
    has_redis = False

from collectors.lib import utils

# If we are root, drop privileges to this user, if necessary.  NOTE: if this is
# not root, this MUST be the user that you run redis-server under.  If not, we
# will not be able to find your Redis instances.
USER = "root"

# Every SCAN_INTERVAL seconds, we look for new redis instances.  Prevents the
# situation where you put up a new instance and we never notice.
SCAN_INTERVAL = 300

# these are the things in the info struct that we care about
KEYS = [
    'pubsub_channels', 'bgrewriteaof_in_progress', 'connected_slaves', 'connected_clients', 'keyspace_misses',
    'used_memory', 'total_commands_processed', 'used_memory_rss', 'total_connections_received', 'pubsub_patterns',
    'used_cpu_sys', 'blocked_clients', 'used_cpu_user', 'expired_keys', 'bgsave_in_progress', 'hash_max_zipmap_entries',
    'hash_max_zipmap_value', 'client_longest_output_list', 'client_biggest_input_buf', 'uptime_in_seconds',
    'changes_since_last_save', 'mem_fragmentation_ratio', 'keyspace_hits', 'evicted_keys'
];


def main():
    """Main loop"""

    if USER != "root":
        utils.drop_privileges(user=USER)
    sys.stdin.close()

    interval = 15

    # we scan for instances here to see if there are any redis servers
    # running on this machine...
    last_scan = time.time()
    instances = scan_for_instances()  # port:name
    if not len(instances):
        return 13
    if not has_redis:
        sys.stderr.write("Found %d instance(s) to monitor, but the Python"
                         " Redis module isn't installed.\n" % len(instances))
        return 1

    def print_stat(metric, value, tags=""):
        if value is not None:
            print "redis.%s %d %s %s" % (metric, ts, value, tags)

    while True:
        ts = int(time.time())

        # if we haven't looked for redis instances recently, let's do that
        if ts - last_scan > SCAN_INTERVAL:
            instances = scan_for_instances()
            last_scan = ts

        # now iterate over every instance and gather statistics
        for port in instances:
            tags = "cluster=%s port=%d" % (instances[port], port)

            # connect to the instance and attempt to gather info
            r = redis.Redis(host="127.0.0.1", port=port)
            info = r.info()
            for key in KEYS:
                if key in info:
                    print_stat(key, info[key], tags)

            # get some instant latency information
            # TODO: might be nice to get 95th, 99th, etc here?
            start_time = time.time()
            r.ping()
            print_stat("latency", time.time() - start_time, tags)

        sys.stdout.flush()
        time.sleep(interval)


def scan_for_instances():
    """Use netstat to find instances of Redis listening on the local machine, then
    figure out what configuration file they're using to name the cluster."""

    out = {}
    tcre = re.compile(r"^\s*#\s*tcollector.(\w+)\s*=\s*(.+)$")

    ns_proc = subprocess.Popen(["netstat", "-tnlp"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, _ = ns_proc.communicate()
    if ns_proc.returncode != 0:
        print >> sys.stderr, "failed to find instances %r" % ns_proc.returncode
        return {}

    for line in stdout.split("\n"):
        if not (line and 'redis-server' in line):
            continue
        pid = int(line.split()[6].split("/")[0])
        port = int(line.split()[3].split(":")[1])

        # now we have to get the command line.  we look in the redis config file for
        # a special line that tells us what cluster this is.  else we default to using
        # the port number which should work.
        cluster = "port-%d" % port
        try:
            f = open("/proc/%d/cmdline" % pid)
            cfg = f.readline().split("\0")[-2]
            f.close()

            f = open(cfg)
            for cfgline in f:
                result = tcre.match(cfgline)
                if result and result.group(1).lower() == "cluster":
                    cluster = result.group(2).lower()
        except EnvironmentError:
            # use the default cluster name if anything above failed.
            pass

        out[port] = cluster
    return out


if __name__ == "__main__":
    sys.exit(main())
