#!/usr/bin/env python
import time
import simplejson as json
import requests
import re
import sys


# Constants
TIME = int(time.time())
URL = 'http://127.0.0.1:8081/metrics'
METRIC_PREFIX = 'optimizely.codahale.'


def get_json(url):
    """ Request URL, load JSON, exit if error. """
    url_json = None
    try:
        r = requests.get(url)
    except Exception, e:
        print 'Unable to query url {} - {}'.format(url, e)
        sys.exit(0)
    if r.status_code == 200:
        try:
            url_json = r.json()
        except Exception, e:
            print 'Could not load JSON for {}'.format(url)
            raise e
    else:
        print 'Did not receive 200 response for {}'.format(url)
    return url_json


def format_tsd_key(metric_key, metric_value, tags={}):
    """ Formats a key for OpenTSDB """
    expanded_tags = ''.join([' %s=%s' % (key, value) for key, value in tags.iteritems()])
    output = '{} {} {} {}'.format(metric_key, TIME, metric_value, expanded_tags)
    return output


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
                print format_tsd_key(METRIC_PREFIX + metric_type + "." + metric, value, {
                                     'class': class_name,
                                     'method': method_name})


if __name__ == '__main__':
    main()
