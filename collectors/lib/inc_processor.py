#!/usr/bin/env python

from collectors.lib.collectorbase import MetricType

# a processor converting a monotonically increasing metric to increments. Example is tomcat's
# GlobalProcessor.processingTime, which measures accumulative latency of requests processing.
# caller is reponsible for maintaining the life span of IncProcessor


class IncPorcessor(object):
    def __init__(self, logger):
        self.logger = logger
        self.prev_val = None
        self.tag = "metric_type=%s" % MetricType.INC

    def process(self, name, ts, val):
        if (self.prev_val is None) or val < self.prev_val:
            if val < self.prev_val:
                self.logger.error("%s: value is not monotonically increasing. prev=%d, current=%d",
                                  name, self.prev_val, val)
            return_str = "%s %d %d %s" % (name, ts, 0, self.tag)
        else:
            return_str = "%s %d %d %s" % (name, ts, val - self.prev_val, self.tag)
        self.prev_val = val
        return return_str
