#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2013  The tcollector Authors.
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

"""Statistics from a Cassandra instance.

Note: this collector parses your Cassandra configuration files to determine what cluster
this instance is part of.  If you want the cluster tag to be accurate, please edit
your Cassandra configuration file and add a comment like this somewhere in the file:

# tcollector.cluster = main

You can name the cluster anything that matches the regex [a-z0-9-_]+.

todo: This collector outputs the following metrics:

For more information on these values, see this documentation:

    http://cassandra.apache.org/doc/latest/operating/metrics.html
"""

import argparse
import cassandra
import copy
import json
import re
import subprocess
import sys
import time
import yaml

# If we are root, drop privileges to this user, if necessary.  NOTE: if this is
# not root, this MUST be the user that you run cassandra server under.  If not, we
# will not be able to find your Cassandra instances.
USER = "root"

# these are the things in nodetool that we care about
stat_families = {
    'info': {
        'args': ['nodetool', 'info'],
        'format': 'yaml',
        'filter': {
            '^Load': 'size_converter',
            '^Uptime': True,
            '^Heap Memory': 'heap_converter',
            '^Off Heap Memory': True,
            '^Exceptions': True,
            '^Key Cache': 'cache_converter',
            '^Row Cache': 'cache_converter',
            '^Counter Cache': 'cache_converter',
            '^Chunk Cache': 'cache_converter'
            }
        },
    'ks': {
        'args': ['nodetool', 'cfstats', '-F', 'json'],
        'format': 'json',
        'filter': {
            '.+': {
                'read_count': True,
                'read_latency_ms': True,
                'write_count': True,
                'write_latency_ms': True,
                'tables': {
                    '.+': {
                        'local_read_count': True,
                        'local_read_latency_ms': True,
                        'local_write_count': True,
                        'local_write_latency_ms': True,
                        'space_used_total': True,
                        'space_used_live': True,
                        'memtable_data_size': True
                        }
                    }
                }
            }
        },
    'gc': {
        'args': ['nodetool', 'gcstats'],
        'format': 'gcstats',
        'filter': {
            '.+': True
            }
        },
    'tp': {
        'args': ['nodetool', 'tpstats', '-F', 'json'],
        'format': 'json',
        'filter': {
            'DroppedMessage': 'dict_sum',
            'ThreadPools': {
                '.+': {
                    'TotalBlockedTasks': True,
                    'CurrentlyBlockedTasks': True,
                    }
                }
            }
        }
    }

cached_regex = {}

size_multiplier = {
    'KiB': 1024,
    'MiB': 1048576,
    'GiB': 1073741824
    }

tags = ''

args = None

# Convert "299.62 KiB" to 306810 (unit is bytes)
def size_converter(value_str):
    global size_multiplier

    parts = value_str.split()
    if len(parts) != 2 or parts[1] not in ('KiB', 'MiB', 'GiB'):
        print >> sys.stderr, "Invalid size format:", value_str
        return None

    return float(parts[0]) * size_multiplier[parts[1]]

# Convert "1541.61 / 7987.25" to 1541.61 (unit is MB)
def heap_converter(value_str):
    parts = value_str.split()
    if len(parts) != 3 or parts[1] != "/":
        print >> sys.stderr, "Invalid heap size format:", value_str
        return None

    return float(parts[0])

# Extract recent hit rate from "entries 166, size 15.27 KiB, capacity 100 MiB, 138 hits, 299 requests, 0.462 recent hit rate, 14400 save period in seconds"
def cache_converter(value_str):
    parts = value_str.split(',')
    regex = re.compile('(\S+) recent hit rate')
    for part in parts:
        match = regex.search(part)
        if match:
            return match.group(1)

    return None

# Convert a dict to the sum of its values
def dict_sum(value_dict):
    if not isinstance(value_dict, dict):
        print >> sys.stderr, "Not a dict:", value_dict
        return None
    else:
        return sum(value_dict.values())

'''
Convert
       Interval (ms) Max GC Elapsed (ms)Total GC Elapsed (ms)Stdev GC Elapsed (ms)   GC Reclaimed (MB)         Collections      Direct Memory Bytes
            28242198                 144                 395                   9          5157984472                   3                       -1
to a dict
'''
def parse_gcstats(value_str):
    lines = value_str.splitlines()
    if len(lines) != 2:
        print >> sys.stderr, "Invalid gcstats format:\n", value_str
        return None

    parts = lines[1].split()
    if len(parts) != 7:
        print >> sys.stderr, "Invalid gcstats value format:", lines[1]
        return None

    return {
        'max_gc_elapsed_ms': int(parts[1]),
        'total_gc_elapsed_ms': int(parts[2])
    }

def get_stats(stat_options):
    try:
        p = subprocess.Popen(stat_options['args'], stdout=subprocess.PIPE)
        output_str, error_str = p.communicate()
        if stat_options['format'] == 'yaml':
            output = yaml.load(output_str)
        elif stat_options['format'] == 'json':
            output = json.loads(output_str)
        elif stat_options['format'] == 'gcstats':
            output = parse_gcstats(output_str)
        else:
            raise "Invalid format " + stat_options['format']
        return output
    except Exception as exc:
        print >> sys.stderr, exc
        return None

def get_cached_regex(pattern):
    global cached_regex

    if cached_regex.has_key(pattern):
        return cached_regex[pattern]
    else:
        regex = re.compile(pattern)
        cached_regex[pattern] = regex
        return regex

def get_sub_filters(filters, name):
    if not isinstance(filters, dict):
        print >> sys.stderr, 'Invalid filters type:', type(filters), 'filters:', filters, 'name:', name
        return None

    for pattern, sub_filters in filters.items():
        regex = get_cached_regex(pattern)
        if regex.search(name):
            return sub_filters

    return None

def print_stat(name, value, ts):
    global args
    global tags

    if value is not None and (args.names or (value != "NaN" and value != u"NaN")):
        print "cas.%s %d %s %s" % (name, ts, value, tags)

def print_stats(path, values, filters, ts):
    if isinstance(values, int) or isinstance(values, float) or isinstance(values, str) or isinstance(filters, bool) or isinstance(filters, str):
        name = '.'.join(path)
        if isinstance(filters, bool) and filters:
            print_stat(name, values, ts)
        elif isinstance(filters, str):
            v = globals().get(filters)(values)
            print_stat(name, v, ts)
        return

    if not isinstance(values, dict):
        print >> sys.stderr, "Invalid values type:", type(values), values
        return

    for name, value in values.items():
        sub_filters = get_sub_filters(filters, name)
        if sub_filters:
            sub_path = copy.copy(path)
            norm_name = re.sub('\W+', '_', name)
            norm_name = norm_name.lower()
            sub_path.append(norm_name)
            print_stats(sub_path, value, sub_filters, ts)

def print_once():
    global stat_families

    ts = int(time.time())

    # Iterate over every stat family and gather statistics
    for stat_name, stat_options in stat_families.items():
        stats = get_stats(stat_options)
        if stats:
            print_stats([stat_name], stats, stat_options['filter'], ts)

def main():
    """Main loop"""

    global tags

    sys.stdin.close()

    from collectors.etc import cassandra_stats_conf
    config = cassandra_stats_conf.get_config()
    interval = config['collection_interval']

    cas_config_options = {
        'args': ['cat', '/etc/cassandra/cassandra.yaml'],
        'format': 'yaml'
    }
    cas_config = get_stats(cas_config_options)
    print >> sys.stderr, 'Cassandra Configuration:', cas_config
    cas_cluster = re.sub('\W+', '_', cas_config['cluster_name'])
    # When migrating to Prometheus, the tag 'cluster' is already used by the k8s clusters, for example
    # 'prod-mgmt'. Add an additional label 'cas_cluster' to represent the cassandra clusters.
    # TODO: once OpenTSDB is fully retired, use 'cas_cluster' to replace 'cluster' tag.
    tags = "cluster=" + cas_cluster
    tags += ' cas_cluster={}'.format(cas_cluster)

    while True:
        print_once()
        sys.stdout.flush()
        time.sleep(interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-n',
        '--names',
        action='store_true',
        default=False,
        help="Print all metric names and exit")
    args = parser.parse_args()

    if args.names:
        sys.exit(print_once())
    else:
        sys.exit(main())
