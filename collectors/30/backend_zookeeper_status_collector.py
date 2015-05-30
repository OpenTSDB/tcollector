#!/usr/bin/env python

import os
import time
from kazoo.client import KazooClient

from collectors.lib.optimizely_utils import format_tsd_key


STATUS_ROOT = '/optimizely/status/'
ELAPSED_SECONDS_METRICS = [
        'metaSync.lastStart',
        'metaSync.lastSuccess',
        'batchCompute.lastStart',
        'batchCompute.lastSuccess',
        'resultsUpload.lastStart',
        'resultsUpload.lastSuccess',
        ]
 

def report():
    zk_quorums = os.getenv('MONITORED_ZOOKEEPER_QUORUMS')
    if zk_quorums is None:
        raise RuntimeError('MONITORED_ZOOKEEPER_QUORUMS not found')

    for zk_quorum in (x for x in zk_quorums.split('|') if x):
        zk = KazooClient(hosts=zk_quorum)
        zk.start()

        for metric in ELAPSED_SECONDS_METRICS:
            metric_path = STATUS_ROOT + metric

            if zk.exists(metric_path) is None:
                continue
        
            value, _ = zk.get(metric_path)
            time_ = int(time.time())
            elapsed = max(0, time_ - int(value) / 1000)
            tags = { 'zkquorum': zk_quorum.replace(',', '_') }
        
            print format_tsd_key(metric + '.elapsedSeconds', elapsed, time_, tags)

        zk.stop()
        zk.close()
 
 
if __name__ == '__main__':
    report()
