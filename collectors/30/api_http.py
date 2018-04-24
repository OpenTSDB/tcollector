#!/usr/bin/env python
import time
import requests
import sys

from collectors.lib.optimizely_utils import format_tsd_key, get_json


# Constants
TIME = int(time.time())
URL = 'http://127.0.0.1:8081/metrics'
METRIC_PREFIX = 'optimizely.codahale.'


def main():
    json = get_json(URL)
    # We only have timers now - not sure if
    # the other ones look the same
    metric_types = ['timers', 'meters', 'counters', 'gauges', 'histograms']
    not_metrics = ['duration_units', 'rate_units', 'units']
    for metric_type in metric_types:
        for classpath, metrics in json[metric_type].iteritems():
            splitpath = classpath.split(".")
            if len(splitpath) < 2:
              continue
            class_name = splitpath[-2]
            method_name = splitpath[-1]
            for metric, value in metrics.iteritems():
                if metric in not_metrics:
                    continue
                print format_tsd_key(METRIC_PREFIX + metric_type + "." + metric, value, TIME, {
                                     'class': class_name,
                                     'method': method_name})


if __name__ == '__main__':
    main()
