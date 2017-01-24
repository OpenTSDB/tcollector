#!/usr/bin/env python
import time
import requests
import commands
import re
import sys
CONSUL_BASEURL = 'http://localhost:8500/v1'
CONSUL_DC = None
def output_metric(metric, value, extra_tags=None):
    timestamp = int(time.time())
    tags = 'consul_dc=%s' % CONSUL_DC
    if extra_tags is not None:
        for tag, tag_value in extra_tags.iteritems():
            tags += ' %s=%s' % (tag, tag_value)
    print "consul.%s %d %s %s" % (metric, timestamp, value, tags)
def query_consul(url_suffix):
    r = requests.get('%s/%s' % (CONSUL_BASEURL, url_suffix))
    r.raise_for_status()
    return r.json()
def main():
    global CONSUL_DC
    foo = query_consul('agent/self')
    CONSUL_DC = foo['Config']['Datacenter']
    peers = query_consul('status/peers')
    output_metric('status.peers', len(peers))
    nodes = query_consul('catalog/nodes')
    output_metric('catalog.nodes', len(nodes))
    services = query_consul('catalog/services')
    output_metric('catalog.services', len(services))
    for service in sorted(services.keys()):
        foo = query_consul('catalog/service/%s' % service)
        output_metric('catalog.service.instances', len(foo), { 'service': service })
    # Parse output of 'consul info'
    output = commands.getoutput('consul info')
    header = None
    for line in output.splitlines():
        if re.match(r'[ \t]+', line):
            # Metric
            (key, val) = [x.strip() for x in line.split(' = ')]
            # Only output numeric values
            matches = re.match(r'(\d+(?:\.\d+)?)(?:[a-z]+)?$', val)
            if not matches is None:
                output_metric('%s.%s' % (header, key), matches.group(1))
        else:
            # Header
            header = line[:-1] # Strip off trailing colon
if __name__ == "__main__":
    try:
        main()
    except:
        # Exit with 0 so that tcollector doesn't mark us dead
        sys.exit(0)

