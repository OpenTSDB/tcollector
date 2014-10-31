#!/usr/bin/env python

import os
import time

from kazoo.client import KazooClient
from kafka import KafkaClient, SimpleConsumer
from kafka.common import TopicAndPartition, OffsetRequest


# TSD UTILITIES


def format_tsd_key(metric_key, metric_value, timestamp, tags={}):
    expanded_tags = ''.join([' %s=%s' % (key, value) for key, value in tags.iteritems()])
    output = '{} {} {} {}'.format(metric_key, timestamp, metric_value, expanded_tags)
    return output


def format_kafka_broker_offset_tsd_key(topic, partition, offset):
    metric = 'kafka.broker.offset'
    tags = dict(topic=topic, partition=partition)
    timestamp = int(time.time())
    return format_tsd_key(metric, offset, timestamp, tags)


def format_kafka_consumer_offset_tsd_key(consumer_group, topic, partition, offset):
    metric = 'kafka.consumer.offset'
    tags = dict(consumer_group=consumer_group, topic=topic, partition=partition)
    timestamp = int(time.time())
    return format_tsd_key(metric, offset, timestamp, tags)


# KAFKA OFFSET MONITORING


def get_partitions(zk, group, topic):
    return zk.get_children(KafkaPaths.topic_offsets(group, topic))


def get_consumer_group_offset(zk, group, topic, partition):
    path = KafkaPaths.consumer_topic_partition(group, topic, partition)
    return int(zk.get(path)[0])


def report_broker_info(kafka, zk, topic):
    def get_partitions(topic):
        path = KafkaPaths.broker_partitions(topic)
        return map(int, zk.get_children(path))

    for partition in get_partitions(topic):
        offset_response = kafka.send_offset_request([OffsetRequest(topic, partition, -1, 1)])[0]
        print format_kafka_broker_offset_tsd_key(topic, partition, offset_response.offsets[0])


def report():
    # see the monitoring.json chef role
    zk_quorums = os.getenv('MONITORED_ZOOKEEPER_QUORUMS')
    kafka_brokers = os.getenv('KAFKA_BROKERS')

    if zk_quorums is None:
        raise RuntimeError('MONITORED_ZOOKEEPER_QUORUMS not found')

    if kafka_brokers is None:
        raise RuntimeError('KAFKA_BROKERS not found')

    # we could just use ZK to find all topics, should we just do that?
    topics = ['mobile_metrics', 'raw_events']

    consumer_group = 'secor_group'

    for zk_quorum in zk_quorums.split('|'):
        zk = KazooClient(hosts=zk_quorum)
        kafka = KafkaClient(kafka_brokers)

        zk.start()
        for topic in topics:
            report_broker_info(kafka, zk, topic)

            for partition in get_partitions(zk, consumer_group, topic):
                offset = get_consumer_group_offset(zk, consumer_group, topic, partition)
                print format_kafka_consumer_offset_tsd_key(consumer_group, topic, partition, offset)
        zk.stop()

        kafka.close()
        zk.close()


class KafkaPaths:
    kafka_chroot = '/kafka'

    @staticmethod
    def topic_offsets(group, topic):
        return '%s/consumers/%s/offsets/%s' % (KafkaPaths.kafka_chroot, group, topic)

    @staticmethod
    def consumer_topic_partition(group, topic, partition):
        return KafkaPaths.topic_offsets(group, topic) + '/' + partition

    @staticmethod
    def broker_partitions(topic):
        return '%s/brokers/topics/%s/partitions' % (KafkaPaths.kafka_chroot, topic)


if __name__ == '__main__':
    report()
