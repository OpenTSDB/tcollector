#!/usr/bin/env python

import os
import time
from kazoo.client import KazooClient


def format_tsd_key(metric_key, metric_value, timestamp, tags={}):
    expanded_tags = ''.join([' %s=%s' % (key, value) for key, value in tags.iteritems()])
    output = '{} {} {} {}'.format(metric_key, timestamp, metric_value, expanded_tags)
    return output


def format_kafka_offset_tsd_key(group, topic, partition, offset):
    metric = 'kafka.offset'
    value = offset
    tags = dict(group=group, topic=topic, partition=partition)
    timestamp = time.time()
    return format_tsd_key(metric, offset, timestamp, tags)


def get_partitions(zk, group, topic):
    """ this only gets those partitions that this consumer group has owners for I think,
    so this means that if the group for whatever reason isn't consuming a partition, then
    we won't report for it, but thats correct behavior anyway and should be caught by
    increasing offset lag """
    return zk.get_children(KafkaPaths.topic_offsets_path(group, topic))


def get_consumer_group_offset(zk, group, topic, partition):
    path = KafkaPaths.topic_partition_offset_path(group, topic, partition)
    return int(zk.get(path)[0])


def report():
    # see the monitoring.json chef role
    zk_quorums = os.getenv('MONITORED_ZOOKEEPER_QUORUMS')
    if zk_quorums is None:
        raise RuntimeError('MONITORED_ZOOKEEPER_QUORUMS not found')

    # we could just use ZK to find all topics, should we just do that?
    topics = ['mobile_metrics', 'raw_events']

    consumer_group = 'secor_group'

    for zk_quorum in zk_quorums.split('|'):
        zk = KazooClient(hosts=zk_quorum)
        zk.start()
        for topic in topics:
            for partition in get_partitions(zk, consumer_group, topic):
                offset = get_consumer_group_offset(zk, consumer_group, topic, partition)
                print format_kafka_offset_tsd_key(consumer_group, topic, partition, offset)


class KafkaPaths:
    kafka_chroot = '/kafka'

    @staticmethod
    def topic_offsets_path(group, topic):
        return '%s/consumers/%s/offsets/%s' % (KafkaPaths.kafka_chroot, group, topic)

    @staticmethod
    def topic_partition_offset_path(group, topic, partition):
        return KafkaPaths.topic_offsets_path(group, topic) + '/' + partition


if __name__ == '__main__':
    report()
