#!/usr/bin/env python

# a processor converting a monotonically increasing metric to increments. Example is tomcat's
# GlobalProcessor.processingTime, which measures accumulative latency of requests processing.
# caller is reponsible for maintaining the life span of IncProcessor


class IncPorcessor(object):
    def __init__(self, logger):
        self.logger = logger
        self.prev_val = None

    def process(self, name, val):
        if (self.prev_val is None) or val < self.prev_val:
            if val < self.prev_val:
                self.logger.error("%s: value is not monotonically increasing. prev=%d, current=%d",
                                  name, self.prev_val, val)
            return_val = 0
        else:
            return_val = val - self.prev_val
        self.prev_val = val
        return return_val
