#!/usr/bin/python

import urllib2
import time
import re
from collectors.lib import utils


def main():
    try:
        utils.drop_privileges()
        # collect period 60 secs
        url = "http://localhost:9999/stats.txt?period=60"
        response = urllib2.urlopen(url)
        content = response.read()
        process(content)

    except Exception:
        pass


def process(content):
    ts = time.time()
    stype=''
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
            if stype == 'counters' or stype == 'gauges':
                val = int(comps[1])
                print("%s %d %d" % (metric_name, ts, val))
            elif stype == 'metrics':
                vals = [sss.strip(" ()") for sss in re.split(',|=', comps[1])]
                val_avg = getMetricValue("average", vals)
                print("%s %d %d" % (metric_name, ts, val_avg))

                val_max = getMetricValue("maximum", vals)
                print("%s.%s %d %d" % (metric_name, "max", ts, val_max))

                val_min = getMetricValue("minimum", vals)
                print("%s.%s %d %d" % (metric_name, "min", ts, val_min))

                val_p99 = getMetricValue("p99", vals)
                print("%s.%s %d %d" % (metric_name, "p99", ts, val_p99))

                val_p999 = getMetricValue("p999", vals)
                print("%s.%s %d %d" % (metric_name, "p999", ts, val_p999))
            else:
                pass


def getMetricValue(agg, vals):
    idx = vals.index(agg)
    return int(vals[idx + 1])


def test():
    content = '''
counters:
  cloudmon-one-sec-counter: 1626
  cloudmon-ten-sec-counter: 163
gauges:
labels:
metrics:
  cloudmon-read-latency: (average=4, count=1626, maximum=1000, minimum=0, p50=4, p90=9, p95=9, p99=409, p999=903, p9999=906, sum=7289)
  cloudmon-test-metric-1: (average=5099, count=1626, maximum=10498, minimum=5, p50=5210, p90=8594, p95=9498, p99=9498, p999=10498, p9999=10498, sum=8291701)
  cloudmon-write-latency: (average=4, count=1626, maximum=1234, minimum=0, p50=5, p90=9, p95=9, p99=90, p999=151, p9999=400, sum=7406)
'''
    process(content)


def dryrun():
    while(True):
        main()
        time.sleep(10)


if __name__ == "__main__":
    main()
    # dryrun()
