#!/usr/bin/python
"""Send docker stats counters to TSDB"""

import json
import socket
import time
import re
from Queue import Queue

from collectors.lib import utils
from collectors.lib.collectorbase import CollectorBase


recv_data_size = 8192


class DockerDaemon(CollectorBase):
    def __init__(self, config, logger, readq):
        super(DockerDaemon, self).__init__(config, logger, readq)
        # create a socket
        self.client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.client.connect("/var/run/docker.sock")
        self.client.settimeout(5)

    def cleanup(self):
        self.safe_close(self.client)

    def __call__(self):
        utils.drop_privileges()
        containers = self.get_container_list()
        for containername in containers:
            self.get_container_stats(containername)

    def get_container_list(self):
        request = "GET /containers/json?all=1 HTTP/1.1\r\n"
        result = self.query_docker_daemon(request)
        return self.process_container_list(result)

    def get_container_stats(self, containername):
        # cmd = "echo -e \"GET /containers/%s/stats?stream=0 HTTP/1.1\\r\\n\" | nc -q 5 -U /var/run/docker.sock" % containerName
        stats_request = "GET /containers/%s/stats?stream=0 HTTP/1.1\r\n" % containername
        result = self.query_docker_daemon(stats_request)
        self.process_stats(containername, result)

    def query_docker_daemon(self, request):
        self.client.send(request)
        self.client.send("\n")

        # What if data_size is not big enough?
        # while (hasData):
        return self.client.recv(recv_data_size)

    # Parse container list json and return the list of container id.
    # At the same time, write metrics about the status of containers (alive:1, dead/exit:0)
    def process_container_list(self, content):
        # result example:
        # HTTP/1.1 200 OK
        # Content-Type: application/json
        # Server: Docker/1.9.1 (linux)
        # Date: Sun, 17 Jan 2016 07:11:23 GMT
        # Content-Length: 1999
        #
        # [{"Id":"ee49f83d0697da17368a4af76a8e92788d16b35e89f445163f9a210ebe9a9279","Names":["/host1"],"Image":"c0fb6f39a935","ImageID":"c0fb6f39a9350a0f632347776933f78684494dd7cbc36ddf1051335fa1faa660","Command":"bash","Created":1453011350,"Ports":[],"Labels":{},"Status":"Up 55 minutes","HostConfig":{"NetworkMode":"default"}},{"Id":"fcc9ba1ec7295f5061fbde10bd370e2e9bd70955f0f97878591296d892ddde41","Names":["/host2"],"Image":"c0fb6f39a935","ImageID":"c0fb6f39a9350a0f632347776933f78684494dd7cbc36ddf1051335fa1faa660","Command":"bash","Created":1452964039,"Ports":[],"Labels":{},"Status":"Exited (137) 13 hours ago","HostConfig":{"NetworkMode":"default"}},{"Id":"0592aed69afec469c3886e911d90ff288e79a4ddbc6f20030f8ad20eac693045","Names":[],"Image":"c0fb6f39a935","ImageID":"c0fb6f39a9350a0f632347776933f78684494dd7cbc36ddf1051335fa1faa660","Command":"bash","Created":1452927320,"Ports":[],"Labels":{},"Status":"Dead","HostConfig":{"NetworkMode":"default"}},{"Id":"547efeb6aca316230ff99babfbfcde49d9f7b1bcfe109f5e308b8121e6fdce5a","Names":["/dreamy_boyd"],"Image":"c0fb6f39a935","ImageID":"c0fb6f39a9350a0f632347776933f78684494dd7cbc36ddf1051335fa1faa660","Command":"bash --name=host1","Created":1452927306,"Ports":[],"Labels":{},"Status":"Exited (2) 24 hours ago","HostConfig":{"NetworkMode":"default"}},{"Id":"1ccddc75b8c26f43449d16e751f8ec64a80a3bb2809c706cce9050a88025ab3d","Names":["/prickly_fermat"],"Image":"c0fb6f39a935","ImageID":"c0fb6f39a9350a0f632347776933f78684494dd7cbc36ddf1051335fa1faa660","Command":"bash --name host1","Created":1452927291,"Ports":[],"Labels":{},"Status":"Exited (2) 24 hours ago","HostConfig":{"NetworkMode":"default"}},{"Id":"9f8319fcc4ec7c1bbe913d0b8129190f879f29cce740f573a111c9fc4260f02d","Names":["/romantic_mcnulty"],"Image":"c0fb6f39a935","ImageID":"c0fb6f39a9350a0f632347776933f78684494dd7cbc36ddf1051335fa1faa660","Command":"bash -name host1","Created":1452927217,"Ports":[],"Labels":{},"Status":"Exited (1) 24 hours ago","HostConfig":{"NetworkMode":"default"}}]

        ts = time.time()
        container_list_json = self.parse_content_to_json(content)
        containernames = []
        total_alive = 0
        for c in container_list_json:
            if len(c[u'Names']) > 0:
                # u'Names' => [u'/host1']
                containername = c[u'Names'][0][1:]
            else:
                # Sometimes Name is empty for a container.
                # We then have to use its Ids.
                containername = c[u'Id']

            containernames.append(containername)

            # To print status of each container. Alive: 1, Dead/Exit: 0
            tag = "container=%s" % containername
            if c[u'Status'].find("Up") != -1:
                self.print_metric("docker/status", ts, 1, tag)
                total_alive += 1
            else:
                self.print_metric("docker/status", ts, 0, tag)

        self.print_metric("docker/number_containers", ts, len(containernames), "")
        self.print_metric("docker/number_containers_running", ts, total_alive, "")
        return containernames

    # Parse stats json and write metrics/counters to console.
    def process_stats(self, containername, statsresp):
        ts = time.time()

        # message content is as following:

        # HTTP/1.1 200 OK
        # Content-Type: application/json
        # Server: Docker/1.9.1 (linux)
        # Date: Sun, 17 Jan 2016 06:16:55 GMT
        # Content-Length: 1638

        # {"read":"2016-01-16T22:16:55.349404243-08:00","precpu_stats":{"cpu_usage":{"total_usage":68031969,"percpu_usage":[28569353,22696001,3625094,13141521],"usage_in_kernelmode":20000000,"usage_in_usermode":40000000},"system_cpu_usage":81825360000000,"throttling_data":{"periods":0,"throttled_periods":0,"throttled_time":0}},"cpu_stats":{"cpu_usage":{"total_usage":68031969,"percpu_usage":[28569353,22696001,3625094,13141521],"usage_in_kernelmode":20000000,"usage_in_usermode":40000000},"system_cpu_usage":81829300000000,"throttling_data":{"periods":0,"throttled_periods":0,"throttled_time":0}},"memory_stats":{"usage":557056,"max_usage":8781824,"stats":{"active_anon":540672,"active_file":4096,"cache":57344,"hierarchical_memory_limit":18446744073709551615,"inactive_anon":12288,"inactive_file":0,"mapped_file":0,"pgfault":5673,"pgmajfault":0,"pgpgin":2042,"pgpgout":1906,"rss":499712,"rss_huge":0,"total_active_anon":540672,"total_active_file":4096,"total_cache":57344,"total_inactive_anon":12288,"total_inactive_file":0,"total_mapped_file":0,"total_pgfault":5673,"total_pgmajfault":0,"total_pgpgin":2042,"total_pgpgout":1906,"total_rss":499712,"total_rss_huge":0,"total_unevictable":0,"total_writeback":0,"unevictable":0,"writeback":0},"failcnt":0,"limit":12268515328},"blkio_stats":{"io_service_bytes_recursive":[],"io_serviced_recursive":[],"io_queue_recursive":[],"io_service_time_recursive":[],"io_wait_time_recursive":[],"io_merged_recursive":[],"io_time_recursive":[],"sectors_recursive":[]},"networks":{"eth0":{"rx_bytes":6299,"rx_packets":77,"rx_errors":0,"rx_dropped":0,"tx_bytes":648,"tx_packets":8,"tx_errors":0,"tx_dropped":0}}}

        # what if response !OK?
        stats = self.parse_content_to_json(statsresp)
        # Parse the json object
        if containername[0] == '/':
            tags = "container=%s" % containername[1:]
        else:
            tags = "container=%s" % containername

        self.print_json("docker", ts, stats, tags)

    def parse_content_to_json(self, content):
        content_split = re.split('\r|\n', content.strip())

        # Find the right element and parse it to json object.
        for c in content_split:
            if c.startswith('{') and c.endswith('}') or c.startswith('[') and c.endswith(']'):
                return json.loads(c)

        return None

    # Recursively loop json object and print metrics as long as it is numerical.
    # Metric names are concatenated with '/'. E.g.,
    #
    # {"read":"0001-01-01T00:00:00Z","precpu_stats":{"cpu_usage":{"total_usage":0,"percpu_usage":null,"usage_in_kernelmode":0,"usage_in_usermode":0},"system_cpu_usage":0,"throttling_data":{"periods":0,"throttled_periods":0,"throttled_time":0}},"cpu_stats":{"cpu_usage":{"total_usage":0,"percpu_usage":null,"usage_in_kernelmode":0,"usage_in_usermode":0},"system_cpu_usage":0,"throttling_data":{"periods":0,"throttled_periods":0,"throttled_time":0}},"memory_stats":{"usage":0,"max_usage":0,"stats":null,"failcnt":0,"limit":0},"blkio_stats":{"io_service_bytes_recursive":null,"io_serviced_recursive":null,"io_queue_recursive":null,"io_service_time_recursive":null,"io_wait_time_recursive":null,"io_merged_recursive":null,"io_time_recursive":null,"sectors_recursive":null}}
    #
    # not collecting string yet 0001-01-01T00:00:00Z
    # docker/memory_stats/usage 1453154526 0 container=prickly_fermat
    # docker/memory_stats/limit 1453154526 0 container=prickly_fermat
    # docker/memory_stats/failcnt 1453154526 0 container=prickly_fermat
    # docker/memory_stats/max_usage 1453154526 0 container=prickly_fermat
    # docker/precpu_stats/cpu_usage/usage_in_usermode 1453154526 0 container=prickly_fermat
    # docker/precpu_stats/cpu_usage/total_usage 1453154526 0 container=prickly_fermat
    # docker/precpu_stats/cpu_usage/usage_in_kernelmode 1453154526 0 container=prickly_fermat
    # docker/precpu_stats/system_cpu_usage 1453154526 0 container=prickly_fermat
    # docker/precpu_stats/throttling_data/throttled_time 1453154526 0 container=prickly_fermat
    # docker/precpu_stats/throttling_data/periods 1453154526 0 container=prickly_fermat
    # docker/precpu_stats/throttling_data/throttled_periods 1453154526 0 container=prickly_fermat
    # docker/cpu_stats/cpu_usage/usage_in_usermode 1453154526 0 container=prickly_fermat
    # docker/cpu_stats/cpu_usage/total_usage 1453154526 0 container=prickly_fermat
    # docker/cpu_stats/cpu_usage/usage_in_kernelmode 1453154526 0 container=prickly_fermat
    # docker/cpu_stats/system_cpu_usage 1453154526 0 container=prickly_fermat
    # docker/cpu_stats/throttling_data/throttled_time 1453154526 0 container=prickly_fermat
    # docker/cpu_stats/throttling_data/periods 1453154526 0 container=prickly_fermat
    # docker/cpu_stats/throttling_data/throttled_periods 1453154526 0 container=prickly_fermat
    def print_json(self, metric_prefix, ts, json_str, tags):
        if json_str is None:
            pass
        elif isinstance(json_str, (int, float, long)):
            self.print_metric(metric_prefix, ts, json_str, tags)
        elif isinstance(json_str, basestring):
            # print "not collecting string yet %s" % json_str
            pass
        elif isinstance(json_str, list):
            i = 0
            for item in json_str:
                self.print_json(("%s/%d" % (metric_prefix, i)), ts, item, tags)
                i += 1
        elif isinstance(json_str, dict):
            for k, v in json_str.items():
                self.print_json(("%s/%s" % (metric_prefix, k)), ts, v, tags)
        else:
            self.log_error("unknown type %s of json object", type(json_str))

    def print_metric(self, metric, ts, value, tags=""):
        if value is not None:
            self._readq.nput("%s %d %s %s" % (metric, ts, value, tags))


def test():
    name = "host1"
    stats_inst = DockerDaemon(None, None, Queue())
    stats_inst.get_container_stats(name)


def dryrun():
    stats_inst = DockerDaemon(None, None, Queue())
    while True:
        stats_inst()
        time.sleep(10)

if __name__ == "__main__":
    pass
