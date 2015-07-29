#!/usr/bin/env python

import os
import subprocess
import sys
import threading
import time

from collectors.lib import utils


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
LIB_DIR = SCRIPT_DIR + '/../../collectors/lib'

DRUID_METRICS_DIR = '/var/log/druid/metrics'
DRUID_ROLES = [
    'coordinator',
    'overlord',
    'historical',
    'broker',
    'realtime'
]


def get_metric_file(role):
    return DRUID_METRICS_DIR + '/' + role + '.log'


def spawn_monitor(role):
    metric_file = get_metric_file(role)
    name = 'monitor for ' + metric_file

    tailor = subprocess.Popen(
        ['tail', '--lines=0', '--follow=name', '--retry', metric_file],
        bufsize=1,  # line buffered
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    aggregator = subprocess.Popen(
        ['java', '-jar', LIB_DIR + '/druid-metric-aggregator.jar',
         '--role', role, '--metric-catalog', LIB_DIR + '/druid-metrics.yaml'],
        bufsize=1,  # line buffered
        stdin=tailor.stdout,
        stdout=subprocess.PIPE
    )

    tailor.stdout.close()  # allow tailor to receive SIGPIPE if aggregator dies
    tailor.stderr.close()  # ignore stderr of tailor

    def terminate(proc):
        if not proc.poll():
            proc.terminate()
            proc.wait()

    def monitor_thread():
        while True:
            line = aggregator.stdout.readline()

            if line:
                print line
            else:
                print >> sys.stderr, '{} has stopped'.format(name)
                terminate(aggregator)
                terminate(tailor)
                break

    threading.Thread(target=monitor_thread, name=name).start()

    return aggregator


def main():
    utils.drop_privileges()

    monitors_dict = {role: None for role in DRUID_ROLES}

    while True:
        for role, monitor in monitors_dict.items():
            if monitor is None or monitor.poll() is not None:
                monitors_dict[role] = spawn_monitor(role)

        time.sleep(5)


if __name__ == '__main__':
    main()
