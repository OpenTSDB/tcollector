#!/usr/bin/python

import urllib2
import time
import re
from collectors.lib import utils


def main():
    utils.drop_privileges()
    url = "http://localhost:9999/stats.txt"
    response = urllib2.urlopen(url)
    content = response.read()
    ts = time.time()
    for s in [tk.strip() for tk in content.splitlines()]:
        if s == 'counters:':
            stype = 'counters'
        elif s == 'metrics:':
            stype = 'metrics'
        elif s == 'gauges:':
            stype = 'gauges'
        elif s == 'labels:':
            stype = 'labels'
        else:
            comps = [ss.strip() for ss in s.split(':')]
            metric_name = comps[0]
            if stype == 'counters':
                val = int(comps[1])
            elif stype == 'metrics':
                vals = [sss.strip() for sss in re.split(',|=', comps[1])]
                val = int(vals[1])
            else:
                raise
            print("%s %d %d" % (metric_name, ts, val))

if __name__ == "__main__":
    main()
