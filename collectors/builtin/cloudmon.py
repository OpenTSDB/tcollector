#!/usr/bin/python

import urllib2
import time
import re
from collectors.lib import utils
from collectors.lib.collectorbase import CollectorBase
from collectors.lib.collectorbase import MetricType


class Cloudmon(CollectorBase):

    def __init__(self, config, logger, readq):
        super(Cloudmon, self).__init__(config, logger, readq)

    def __call__(self):
        try:
            utils.drop_privileges()
            # collect period 60 secs
            url = self.get_config('stats_url', 'http://localhost:9999/stats.txt')
            response = urllib2.urlopen(url)
            content = response.read()
            return self.process(content)
        except:
            self.log_exception('unexpected error.')

    def process(self, content):
        ts = time.time()
        stype = ''
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
                    if stype == 'counters':
                        self._readq.nput("%s %d %d metric_type=%s" % (metric_name, ts, val, MetricType.COUNTER))
                    else:
                        self._readq.nput("%s %d %d" % (metric_name, ts, val))
                elif stype == 'metrics':
                    vals = [sss.strip(" ()") for sss in re.split(',|=', comps[1])]

                    if "average" in vals:
                        val_avg = self.get_metric_value("average", vals)
                        self._readq.nput("%s %d %d" % (metric_name, ts, val_avg))

                    if "maximum" in vals:
                        val_max = self.get_metric_value("maximum", vals)
                        self._readq.nput("%s.%s %d %d" % (metric_name, "max", ts, val_max))

                    if "minimum" in vals:
                        val_min = self.get_metric_value("minimum", vals)
                        self._readq.nput("%s.%s %d %d" % (metric_name, "min", ts, val_min))

                    if "p99" in vals:
                        val_p99 = self.get_metric_value("p99", vals)
                        self._readq.nput("%s.%s %d %d" % (metric_name, "p99", ts, val_p99))

                    if "p999" in vals:
                        val_p999 = self.get_metric_value("p999", vals)
                        self._readq.nput("%s.%s %d %d" % (metric_name, "p999", ts, val_p999))
                else:
                    self.log_warn('unexpected metric type %s', stype)
                    pass

    def get_metric_value(self, agg, vals):
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
    from Queue import Queue
    cloudmon = Cloudmon(None, None, Queue())
    cloudmon.process(content)


def dryrun():
    from Queue import Queue
    cloudmon_inst = Cloudmon(None, None, Queue())
    while True:
        cloudmon_inst()
        time.sleep(10)


if __name__ == "__main__":
    dryrun()
