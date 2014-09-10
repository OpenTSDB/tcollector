#!/usr/bin/env python

import os
import time
from kazoo.client import KazooClient


STATUS_ROOT = '/optimizely/status/'
ELAPSED_SECONDS_METRICS = [
        'metaSync.lastStart',
        'metaSync.lastSuccess',
        'metaSync.lastSuccess',
        'batchCompute.lastStart',
        'batchCompute.lastSuccess',
        'resultsUpload.lastStart',
        'resultsUpload.lastSuccess',
        ]
 

def format_tsd_key(metric_key, metric_value, time_, tags={}):
    """ Formats a key for OpenTSDB """
    expanded_tags = ''.join([' %s=%s' % (key, value) for key, value in tags.iteritems()])
    output = '{} {} {} {}'.format(metric_key, time_, metric_value, expanded_tags)
    return output
 
 
def report():
    zk_quorum = os.getenv('ZOOKEEPER_QUORUM')
    if zk_quorum is None:
        raise RuntimeError('ZOOKEEPER_QUORUM not found')

    zk = KazooClient(hosts=zk_quorum)
    zk.start()

    for metric in ELAPSED_SECONDS_METRICS:
        metric_path = STATUS_ROOT + metric

        if zk.exists(metric_path) is None:
            continue
    
        value, _ = zk.get(metric_path)
        time_ = int(time.time())
        elapsed = max(0, time_ - int(value) / 1000)
    
        print format_tsd_key(metric + '.elapsedSeconds', elapsed, time_)

    zk.stop()
 
 
if __name__ == '__main__':
    report()
