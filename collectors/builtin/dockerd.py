#!/usr/bin/python
"""Send docker stats counters to TSDB"""

import docker
import time
import re
from Queue import Queue

from collectors.lib import utils
from collectors.lib.collectorbase import CollectorBase


recv_data_size = 8192


class Dockerd(CollectorBase):
    def __init__(self, config, logger, readq):
        super(Dockerd, self).__init__(config, logger, readq)
        self.client = docker.DockerClient(base_url='unix://var/run/docker.sock', timeout=5)

    def cleanup(self):
        pass

    def __call__(self):
        with utils.lower_privileges(self._logger):
            containers = self.get_container_list()
            for containername in containers:
                self.get_container_stats(containername)

    def get_container_list(self):
        ts = time.time()
        running = 0
        containers = self.client.containers.list(all=True)
        names = []
        for container in containers:
            if container.name:
                name = container.name
            else:
                name = container.short_id
            names.append(name)
            tag = "container=%s" % name
            if container.status == u'running':
                self.print_metric("docker.status", ts, 1, tag)
                running += 1
            else:
                self.print_metric("docker.status", ts, 0, tag)
        self.print_metric("docker.number_containers", ts, len(containers), "")
        self.print_metric("docker.number_containers_running", ts, running, "")
        return names

    def get_container_stats(self, containername):
        container = self.client.containers.get(containername)
        stats = container.stats(decode=True, stream=False)
        self.process_stats(containername, stats)

    def query_docker_daemon(self, request):
        self.client.send(request)
        self.client.send("\n")

        # What if data_size is not big enough?
        # while (hasData):
        #return self.client.recv(recv_data_size)
        recv_data = ""
        data = True
        while data:
            data = self.client.recv(recv_data_size)
            recv_data += data
        return recv_data

    # Parse stats json and write metrics/counters to console.
    def process_stats(self, containername, stats):
        ts = time.time()

        # message content is as following:

        # {u'blkio_stats': {u'io_service_time_recursive': [], u'sectors_recursive': [], u'io_service_bytes_recursive': [{u'major': 202, u'value': 16166912, u'minor': 0, u'op': u'Read'}, {u'major': 202, u'value': 0, u'minor': 0, u'op': u'Write'}, {u'major': 202, u'value': 0, u'minor': 0, u'op': u'Sync'}, {u'major': 202, u'value': 16166912, u'minor': 0, u'op': u'Async'}, {u'major': 202, u'value': 16166912, u'minor': 0, u'op': u'Total'}], u'io_serviced_recursive': [{u'major': 202, u'value': 590, u'minor': 0, u'op': u'Read'}, {u'major': 202, u'value': 0, u'minor': 0, u'op': u'Write'}, {u'major': 202, u'value': 0, u'minor': 0, u'op': u'Sync'}, {u'major': 202, u'value': 590, u'minor': 0, u'op': u'Async'}, {u'major': 202, u'value': 590, u'minor': 0, u'op': u'Total'}], u'io_time_recursive': [], u'io_queue_recursive': [], u'io_merged_recursive': [], u'io_wait_time_recursive': []}, u'precpu_stats': {u'cpu_usage': {u'usage_in_usermode': 570000000, u'total_usage': 854627146, u'percpu_usage': [301044061, 553583085], u'usage_in_kernelmode': 240000000}, u'system_cpu_usage': 36754370000000, u'online_cpus': 2, u'throttling_data': {u'throttled_time': 0, u'periods': 0, u'throttled_periods': 0}}, u'name': u'/grokd', u'read': u'2017-05-16T10:38:53.995600562Z', u'storage_stats': {}, u'networks': {u'eth0': {u'tx_dropped': 0, u'rx_packets': 60, u'rx_bytes': 7608, u'tx_errors': 0, u'rx_errors': 0, u'tx_bytes': 648, u'rx_dropped': 0, u'tx_packets': 8}}, u'num_procs': 0, u'preread': u'2017-05-16T10:38:52.994939226Z', u'memory_stats': {u'usage': 44097536, u'limit': 4117659648, u'stats': {u'unevictable': 0, u'total_inactive_file': 12607488, u'total_rss_huge': 0, u'writeback': 0, u'total_cache': 16273408, u'total_mapped_file': 8392704, u'mapped_file': 8392704, u'pgfault': 11834, u'total_writeback': 0, u'hierarchical_memory_limit': 9223372036854771712, u'total_active_file': 3559424, u'rss_huge': 0, u'cache': 16273408, u'active_anon': 27381760, u'pgmajfault': 96, u'total_pgpgout': 1984, u'pgpgout': 1984, u'total_active_anon': 27381760, u'total_unevictable': 0, u'total_pgfault': 11834, u'total_pgmajfault': 96, u'total_inactive_anon': 548864, u'inactive_file': 12607488, u'pgpgin': 12750, u'total_dirty': 0, u'total_pgpgin': 12750, u'rss': 27824128, u'active_file': 3559424, u'inactive_anon': 548864, u'dirty': 0, u'total_rss': 27824128}, u'max_usage': 44302336}, u'pids_stats': {u'current': 3}, u'id': u'a27cb5e5694deb75c04d891b5c5a7450785f3fe1dc16b7707a421990c189902e', u'cpu_stats': {u'cpu_usage': {u'usage_in_usermode': 570000000, u'total_usage': 854711036, u'percpu_usage': [301044061, 553666975], u'usage_in_kernelmode': 240000000}, u'system_cpu_usage': 36756380000000, u'online_cpus': 2, u'throttling_data': {u'throttled_time': 0, u'periods': 0, u'throttled_periods': 0}}}

        # Parse the json object
        if containername[0] == '/':
            tags = "container=%s" % containername[1:]
        else:
            tags = "container=%s" % containername

        self.process_stat("docker", ts, stats, tags)

    def process_stat(self, metric_prefix, ts, stats, tags):
        if stats is None:
            pass
        elif isinstance(stats, dict):
            for key, value in stats.items():
                if isinstance(value, (int, float, long)):
                    self.print_metric(metric_prefix, ts, value, tags)
                elif isinstance(stats, dict):
                    metric_prefix += "." + key
                    self.process_stat(metric_prefix, ts, value, tags)
                elif isinstance(stats, list):
                    metric_prefix += "." + key
                    for v in value:
                        self.process_stat(metric_prefix, ts, v, tags)
        elif isinstance(stats, list):
            for s in stats:
                self.process_stat(metric_prefix, ts, s, tags)

    def print_metric(self, metric, ts, value, tags=""):
        if value is not None:
            # this is the max int OpenTSDB can handle
            if value > 9223372036854775807:
                self.log_warn("The value %d of metric %s is too big. Setting it to 9223372036854775807" % (value, metric))
                value = 9223372036854775807
            self._readq.nput("%s %d %s %s" % (metric, ts, value, tags))


def test():
    name = "host1"
    stats_inst = Dockerd(None, None, Queue())
    stats_inst.get_container_stats(name)


def dryrun():
    stats_inst = Dockerd(None, None, Queue())
    while True:
        stats_inst()
        time.sleep(10)

if __name__ == "__main__":
    pass
