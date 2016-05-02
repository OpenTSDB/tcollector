#!/usr/bin/env python

"""
Couchbase collector

Refer to the following cbstats documentation for more details:

http://docs.couchbase.com/couchbase-manual-2.1/#cbstats-tool
"""

import os
import time
import subprocess
import re

from collectors.lib import utils
from collectors.lib.collectorbase import CollectorBase

KEYS = frozenset([
    'bucket_active_conns',
    'cas_hits',
    'cas_misses',
    'cmd_get',
    'cmd_set',
    'curr_connections',
    'curr_conns_on_port_11209',
    'curr_conns_on_port_11210',
    'ep_queue_size',
    'ep_num_value_ejects',
    'ep_num_eject_failures',
    'ep_oom_errors',
    'ep_tmp_oom_errors',
    'get_hits',
    'get_misses',
    'mem_used',
    'total_connections',
    'total_heap_bytes',
    'total_free_bytes',
    'total_allocated_bytes',
    'total_fragmentation_bytes',
    'tcmalloc_current_thread_cache_bytes',
    'tcmalloc_max_thread_cache_bytes',
    'tcmalloc_unmapped_bytes',
])


def list_bucket(bin_dir):
    """Returns the list of memcached or membase buckets"""
    buckets = []
    if not os.path.isfile("%s/couchbase-cli" % bin_dir):
        return buckets
    cli = ("%s/couchbase-cli" % bin_dir)
    try:
        buck = subprocess.check_output([cli, "bucket-list", "--cluster",
                                        "localhost:8091"])
    except subprocess.CalledProcessError:
        return buckets
    regex = re.compile("[\s\w]+:[\s\w]+$")
    for i in buck.splitlines():
        if not regex.match(i):
            buckets.append(i)
    return buckets


def collect_stats(bin_dir, bucket, ret_metrics):
    """Returns statistics related to a particular bucket"""
    if not os.path.isfile("%s/cbstats" % bin_dir):
        return
    cli = ("%s/cbstats" % bin_dir)
    try:
        ts = time.time()
        stats = subprocess.check_output([cli, "localhost:11211", "-b", bucket,
                                         "all"])
    except subprocess.CalledProcessError:
        return
    for stat in stats.splitlines():
        metric = stat.split(":")[0].lstrip(" ")
        value = stat.split(":")[1].lstrip(" \t")
        if metric in KEYS:
            ret_metrics.append("couchbase.%s %i %s bucket=%s" % (metric, ts, value, bucket))


class Couchbase(CollectorBase):
    def __init__(self, config, logger):
        super(Couchbase, self).__init__(config, logger)
        utils.drop_privileges()

        couchbase_initfile = self.get_config('couchbase_initfile', '/etc/init.d/couchbase-server')
        pid = self.find_couchbase_pid(couchbase_initfile)
        if not pid:
            self.log_error("Error: Either couchbase-server is not running or file (%s) doesn't exist",
                           couchbase_initfile)
            raise

        conf_file = self.find_conf_file(pid)
        if not conf_file:
            self.log_error("Error: Can't find config file (%s)", conf_file)
            raise

        self.bin_dir = self.find_bindir_path(conf_file)
        if not self.bin_dir:
            self.log_error("Error: Can't find bindir path in config file")
            raise

    def __call__(self):
        ret_metrics = []
        # Listing bucket everytime so as to start collecting datapoints
        # of any new bucket.
        buckets = list_bucket(self.bin_dir)
        for b in buckets:
            collect_stats(self.bin_dir, b, ret_metrics)
        return ret_metrics

    def find_couchbase_pid(self, couchbase_initfile):
        if not os.path.isfile(couchbase_initfile):
            return

        try:
            with open(couchbase_initfile) as fd:
                for line in fd:
                    if line.startswith("exec"):
                        init_script = line.split()[1]
        except IOError:
            self.log_error("Check permission of file (%s)", couchbase_initfile)
            return

        try:
            with open(init_script) as fd:
                for line in fd:
                    if line.startswith("PIDFILE"):
                        pid_file = line.split("=")[1].rsplit()[0]
        except IOError:
            self.log_error("Check permission of file (%s)", init_script)
            return

        try:
            with open(pid_file) as fd:
                pid = fd.read()
        except IOError:
            self.log_error("Couchbase-server is not running, since no pid file exists")
            return

        return pid.split()[0]

    def find_conf_file(self, pid):
        """Returns config file for couchbase-server."""
        try:
            fd = open('/proc/%s/cmdline' % pid)
        except IOError, e:
            self.log_error("Couchbase (pid %s) went away ? %s", pid, e)
            return
        try:
            config = fd.read().split("config_path")[1].split("\"")[1]
            return config
        finally:
            fd.close()

    def find_bindir_path(self, config_file):
        """Returns the bin directory path"""
        try:
            fd = open(config_file)
        except IOError, e:
            self.log_error("Error for Config file (%s): %s", config_file, e)
            return None
        try:
            for line in fd:
                if line.startswith("{path_config_bindir"):
                    return line.split(",")[1].split("\"")[1]
        finally:
            fd.close()


if __name__ == "__main__":
    couchbase_inst = Couchbase(None, None)
    couchbase_inst()
