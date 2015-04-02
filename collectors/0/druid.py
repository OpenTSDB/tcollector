#!/usr/bin/env python

import calendar
import json
import re
import subprocess
import sys
import threading
import time

from dateutil import parser as dtparser


DRUID_METRICS_DIR = '/var/log/druid/metrics'
DRUID_ROLES = [
        'coordinator',
        'overlord',
        'historical',
        'broker',
        'realtime'
        ]
 
EVENT_REGEX = re.compile('Event \[(.*)\]')
ILLEGAL_CHARS_REGEX = re.compile('[^a-zA-Z0-9\-_./]')


def format_tsd_key(metric_key, metric_value, time_, tags={}):
    def sanitize(s):
        return ILLEGAL_CHARS_REGEX.sub('_', s)

    expanded_tags = ''.join([' {}={}'.format(sanitize(key), sanitize(value)) for key, value in tags.iteritems()])
    output = '{} {} {} {}'.format(sanitize(metric_key), time_, metric_value, expanded_tags)
    return output
 

def report(line):
    match = EVENT_REGEX.match(line.strip())

    if not match:
        return
    
    event = json.loads(match.group(1))

    metric = 'druid.' + event['metric'].replace('/', '.')
    value = event['value']
    timestamp = calendar.timegm(dtparser.parse(event['timestamp']).utctimetuple())

    tags = event.copy()

    del tags['metric']
    del tags['value']
    del tags['timestamp']

    print format_tsd_key(metric, value, timestamp, tags)

 
def spawn_monitor(metric_file):
    name = 'monitor for ' + metric_file

    monitor = subprocess.Popen(
            ['/usr/bin/tail', '--lines=0', '--follow=name', '--retry', metric_file],
            bufsize=1,  # line buffered
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
            )

    # ignore stderr
    monitor.stderr.close()

    def monitor_thread():
        while True:
            line = monitor.stdout.readline()

            if line:
                report(line)
            else:
                print >> sys.stderr, '{} has stopped'.format(name)
                if not monitor.poll():
                    monitor.terminate()
                    monitor.wait()
                break

    threading.Thread(target=monitor_thread, name=name).start()

    return monitor


def main():
    metric_files_iter = ( DRUID_METRICS_DIR + '/' + role + '.log' for role in DRUID_ROLES )
    monitors_dict = { metric_file: None for metric_file in metric_files_iter }

    while True:
        for metric_file, monitor in monitors_dict.items():
            if monitor is None or monitor.poll() is not None:
                monitors_dict[metric_file] = spawn_monitor(metric_file)

        time.sleep(5)


if __name__ == '__main__':
    main()
