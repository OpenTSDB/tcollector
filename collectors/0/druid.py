#!/usr/bin/env python

import calendar
import json
import re
import subprocess
import sys
import threading
import time

from dateutil import parser as dtparser

from collectors.lib import utils
from collectors.lib.optimizely_utils import format_tsd_key


DRUID_METRICS_DIR = '/var/log/druid/metrics'
DRUID_ROLES = [
        'coordinator',
        'overlord',
        'historical',
        'broker',
        'realtime'
        ]
 
EVENT_REGEX = re.compile('Event \[(.*)\]')

COMMON_TAGS = {'service', 'host'}

# Druid will revamp their metric tagging with meaningful names. Before then, refer to this link
# https://docs.google.com/spreadsheets/d/15XxGrGv2ggdt4SnCoIsckqUNJBZ9ludyhC9hNQa3l-M/edit#gid=0
METRIC_SPECIFIC_TAGS = {
        'request/time':                     {'user2', 'user4', 'user6'},
        'server/segment/used':              {'user1', 'user2'},
        'server/segment/usedPercent':       {'user1', 'user2'},
        'server/segment/count':             {'user1', 'user2'},
        'server/segment/totalUsed':         {'user2'},
        'server/segment/totalUsedPercent':  {'user2'},
        'server/segment/totalCount':        {'user2'},
        'events/thrownAway':                {'user2'},
        'events/unparseable':               {'user2'},
        'events/processed':                 {'user2'},
        'rows/output':                      {'user2'},
        'coordinator/segment/size':         {'user1'},
        'coordinator/segment/count':        {'user1'},
        'coordinator/loadQueue/size':       {'user1'},
        'coordinator/loadQueue/failed':     {'user1'},
        'coordinator/loadQueue/count':      {'user1'},
        'coordinator/dropQueue/count':      {'user1'},
        'indexer/time/run/millis':          {'user2', 'user3', 'user4'},
        'indexer/segment/bytes':            {'user2', 'user4'},
        'indexer/segmentMoved/bytes':       {'user2', 'user4'},
        'indexer/segmentNuked/bytes':       {'user2', 'user4'}
        }


def report(line):
    match = EVENT_REGEX.match(line.strip())

    if not match:
        return
    
    event = json.loads(match.group(1))

    druid_metric = event['metric']
    metric = 'druid.' + druid_metric.replace('/', '.')
    value = event['value']
    timestamp = calendar.timegm(dtparser.parse(event['timestamp']).utctimetuple())

    tags_to_include = COMMON_TAGS
    metric_specific_tags = METRIC_SPECIFIC_TAGS.get(druid_metric)
    if metric_specific_tags:
        tags_to_include = tags_to_include | metric_specific_tags

    tags = { k: v for k, v in event.iteritems() if k in tags_to_include }

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
    utils.drop_privileges()

    metric_files_iter = ( DRUID_METRICS_DIR + '/' + role + '.log' for role in DRUID_ROLES )
    monitors_dict = { metric_file: None for metric_file in metric_files_iter }

    while True:
        for metric_file, monitor in monitors_dict.items():
            if monitor is None or monitor.poll() is not None:
                monitors_dict[metric_file] = spawn_monitor(metric_file)

        time.sleep(5)


if __name__ == '__main__':
    main()
