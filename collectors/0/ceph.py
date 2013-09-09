#!/usr/bin/python

# ceph.py -- a Ceph storage cluster collector for tcollector/OpenTSDB
# Copyright (C) 2013  Berant Lemmenes, berant@lemmenes.com
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


try:
    import json
    json
except ImportError:
    import simplejson as json

import glob
import os
import re
import sys
import subprocess
import time

interval = 15  # seconds

config = {
    'socket_path': '/var/run/ceph',
    'socket_prefix': 'ceph-',
    'socket_ext': 'asok',
    'ceph_binary': '/usr/bin/ceph',
}


def log_error(msg):
    print >>sys.stderr, msg


def flatten_dictionary(input, sep='.', prefix=None):
    """Produces iterator of pairs where the first value is
    the joined key names and the second value is the value
    associated with the lowest level key. 
    """
    for name, value in sorted(input.items()):
        fullname = sep.join(filter(None, [prefix, name]))
        if isinstance(value, dict):
            for result in flatten_dictionary(value, sep, fullname):
                yield result
        else:
            yield (fullname, value)


def get_socket_paths():
    """Return a list of sockets for communicating
    with all of the ceph daemons running on a host.
    """
    socket_pattern = os.path.join(config['socket_path'],
                                  (config['socket_prefix']
                                   + '*.' + config['socket_ext']))
    if len(glob.glob(socket_pattern)) > 0:
        return glob.glob(socket_pattern)
    else:
        log_error("No ceph sockets found at %s, exiting." %
                  (config['socket_path']))
        sys.exit(13)  # no sockets found ask tcollector to not run


def get_prefix_from_socket(name):
    """Given the name of a socket this will return the prefix of the socket.

    In actuallity this prefix is also the name of the cluster (ceph by default)
    """
    base = os.path.splitext(os.path.basename(name))[0]
    if base.startswith(config['socket_prefix']):
        base = base[len(config['socket_prefix']):]
    return 'ceph.' + base


def get_metrics_from_socket(name):
    """Return the parsed JSON data received from the admin socket.

    In the event of an error error, the exception is logged, and
    an empty result set is returned.
    """
    try:
        # Uses the ceph binary to dump the metrics from the admin socket
        json_blob = subprocess.check_output(
            [config['ceph_binary'],
             '--admin-daemon',
             name,
             'perf',
             'dump',
             ])
    except subprocess.CalledProcessError, err:
        log_error("Could not get metrics from %s: %s" %
                  (name, err))
        return {}

    try:
        json_data = json.loads(json_blob)
    except Exception, err:
        log_error("Could not parse metrics from %s: %s" %
                  (name, err))
        return {}

    return json_data


def print_metrics(cluster_name, stats, ts):
    """ Print the metrics retrived from the admin socket.

    Prints in the following format as expected by tcollector:

    metricname time(epoch) value cluster=name daemon=name

    """
    for stat_name, stat_value in flatten_dictionary(
        stats,
        prefix=cluster_name,
    ):
        cluster, daemon, num_id, metric = stat_name.split('.', 3)
        daemon_id = '.'.join([daemon, num_id])
        metric = re.sub('::', '.', metric)
        print ("ceph.%s %d %s cluster=%s daemon=%s"
               % (metric, ts, stat_value, cluster, daemon_id))


def main():
    while True:
        for path in get_socket_paths():
            # pull the cluster name (socket prefix)
            cluster_name = get_prefix_from_socket(path)
            ts = int(time.time())
            # retreive metrics from the admin socket
            stats = get_metrics_from_socket(path)
            # print metrics in OpenTSDB format
            print_metrics(cluster_name, stats, ts)

        sys.stdout.flush()
        time.sleep(interval)


if __name__ == "__main__":
    sys.exit(main())
