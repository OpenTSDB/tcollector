#!/usr/bin/python
#
# collector for Riak statistics.  this is very basic right now.  borrowed
# somewhat from netstat.py.
#
# written by Mark Smith <mark@bu.mp>
#


"""Statistics from a Riak node.

The following all have tags of 'type' which can be 'get' or 'put'.  Latency
is measured in milliseconds.  All latency values are calculated over the last
60 seconds and are moving values.

 - riak.vnode.requests
 - riak.node.requests
 - riak.node.latency.mean
 - riak.node.latency.median
 - riak.node.latency.95th
 - riak.node.latency.99th
 - riak.node.latency.100th

These metrics have no tags and are global:

 - riak.memory.total
 - riak.memory.allocated
 - riak.executing_mappers
 - riak.sys_process_count
 - riak.read_repairs
 - riak.connections
 - riak.connected_nodes
"""

import json
import os
import pwd
import re
import resource
import sys
import time
import urllib2

# If we're running as root and this user exists, we'll drop privileges.  Set this
# to 'root' if you don't want to drop privileges.
USER = "nobody"

MAP = {
    'vnode_gets_total': ('vnode.requests', 'type=get'),
    'vnode_puts_total': ('vnode.puts', 'type=put'),
    'node_gets_total': ('node.requests', 'type=get'),
    'node_puts_total': ('node.requests', 'type=put'),
    'node_get_fsm_time_mean': ('node.latency.mean', 'type=get'),
    'node_get_fsm_time_median': ('node.latency.median', 'type=get'),
    'node_get_fsm_time_95': ('node.latency.95th', 'type=get'),
    'node_get_fsm_time_99': ('node.latency.99th', 'type=get'),
    'node_get_fsm_time_100': ('node.latency.100th', 'type=get'),
    'node_put_fsm_time_mean': ('node.latency.mean', 'type=put'),
    'node_put_fsm_time_median': ('node.latency.median', 'type=put'),
    'node_put_fsm_time_95': ('node.latency.95th', 'type=put'),
    'node_put_fsm_time_99': ('node.latency.99th', 'type=put'),
    'node_put_fsm_time_100': ('node.latency.100th', 'type=put'),
    'pbc_connects_total': ('connections', ''),
    'read_repairs_total': ('read_repairs', ''),
    'sys_process_count': ('sys_process_count', ''),
    'executing_mappers': ('executing_mappers', ''),
    'mem_allocated': ('memory.allocated', ''),
    'mem_total': ('memory.total', ''),
    #connected_nodes is calculated
}

def drop_privileges():
    """Drops privileges if running as root."""

    if USER == 'root':
        return

    try:
        ent = pwd.getpwnam(USER)
    except KeyError:
        return

    if os.getuid() != 0:
        return
    os.setgid(ent.pw_gid)
    os.setuid(ent.pw_uid)


def main():
    """Main loop"""

    # don't run if we're not a riak node
    if not os.path.exists("/usr/lib/riak"):
        sys.exit(13)

    drop_privileges()
    sys.stdin.close()

    interval = 15

    def print_stat(metric, value, tags=""):
        if value is not None:
            print "riak.%s %d %s %s" % (metric, ts, value, tags)

    while True:
        ts = int(time.time())

        req = urllib2.urlopen("http://localhost:8098/stats")
        if req is not None:
            obj = json.loads("".join(req.readlines()))
            for key in obj:
                if key in MAP:
                    # this is a hack, but Riak reports latencies in microseconds.  they're fairly useless
                    # to our human operators, so we're going to convert them to seconds.
                    if 'latency' in MAP[key][0]:
                        obj[key] = float(obj[key] / 1000000.0)
                    print_stat(MAP[key][0], obj[key], MAP[key][1])
            if 'connected_nodes' in obj:
                print_stat('connected_nodes', len(obj['connected_nodes']), '')

        sys.stdout.flush()
        time.sleep(interval)


if __name__ == "__main__":
    sys.exit(main())
