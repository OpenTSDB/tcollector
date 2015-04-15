#!/usr/bin/env python

import os
import time
import json
import logging

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kafka import KafkaClient, SimpleConsumer
from kafka.common import TopicAndPartition, OffsetRequest, LeaderNotAvailableError


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


def format_kafka_consumer_lag_tsd_key(consumer_group, topic, lag):
    metric = 'kafka.lag'
    tags = dict(consumer_group=consumer_group, topic=topic)
    timestamp = int(time.time())
    return format_tsd_key(metric, lag, timestamp, tags)


def format_kafka_consumer_offset_tsd_key(consumer_group, topic, partition, offset):
    metric = 'kafka.consumer.offset'
    tags = dict(consumer_group=consumer_group, topic=topic, partition=partition)
    timestamp = int(time.time())
    return format_tsd_key(metric, offset, timestamp, tags)


# KAFKA OFFSET MONITORING


def get_partitions(zk, group, topic):
    try:
        return map(int, zk.get_children(KafkaPaths.topic_offsets(group, topic)))
    except NoNodeError as err:
        logging.exception(err)
        return []

def get_kafka_topics(zk):
    """
    Return a list of all kafka topics, or an empty list if the topics cannot be fetched
    """
    try:
        return map(str, zk.get_children(KafkaPaths.topics()))
    except NoNodeError as err:
        logging.exception(err)
        return []

def get_kafka_brokers(zk):
    """
    Return a list of all kafka broker hosts or an empty list if the broker list cannot be fetched
    """
    brokers = []
    for broker_id in zk.get_children(KafkaPaths.broker_ids()):
        broker_desc = json.loads(zk.get(KafkaPaths.broker(broker_id))[0])
        brokers.append(broker_desc['host'].split('.')[0])
    return brokers

def get_consumer_group_offset(zk, group, topic, partition):
    path = KafkaPaths.consumer_topic_partition(group, topic, partition)
    return int(zk.get(path)[0])


def report_broker_info(kafka, zk, topic):
    def get_partitions(topic):
        path = KafkaPaths.broker_partitions(topic)
        return map(int, zk.get_children(path))

    total_offset = 0
    for partition in get_partitions(topic):
        offset_response = kafka.send_offset_request([OffsetRequest(topic, partition, -1, 1)])[0]
        offset = offset_response.offsets[0]
        total_offset += offset
        print format_kafka_broker_offset_tsd_key(topic, partition, offset)
    return total_offset


def report():
    # see the monitoring.json chef role
    zk_quorums = os.getenv('MONITORED_ZOOKEEPER_QUORUMS')

    if zk_quorums is None:
        raise RuntimeError('MONITORED_ZOOKEEPER_QUORUMS not found')

    consumer_group = 'secor_group'

    for zk_quorum in zk_quorums.split('|'):
        zk = KazooClient(hosts=zk_quorum)
        zk.start()

        kafka_brokers = get_kafka_brokers(zk)
        if not kafka_brokers:
            raise RuntimeError('KAFKA_BROKERS could not be fetched from ZK')
        kafka_init = 'env=production kafka=%s zk=%s' % (','.join(kafka_brokers), zk_quorum)
        kafka = KafkaClient(kafka_init)

        # Pull topics from zookeeper
        # e.g. topics = ['mobile_metrics', 'raw_events']
        topics = get_kafka_topics(zk)
        if not topics:
            raise RuntimeError('KAFKA_TOPICS could not be fetched from ZK')
        for topic in topics:
            try:
                total_broker_offset = report_broker_info(kafka, zk, topic)
            except LeaderNotAvailableError as err:
                logging.exception(err)
                total_broker_offset = 0

            total_consumer_offset = 0
            for partition in get_partitions(zk, consumer_group, topic):
                offset = get_consumer_group_offset(zk, consumer_group, topic, partition)
                total_consumer_offset += offset
                print format_kafka_consumer_offset_tsd_key(consumer_group, topic, partition, offset)
            print format_kafka_consumer_lag_tsd_key(consumer_group, topic, total_broker_offset - total_consumer_offset)
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
        return '%s/%d' % (KafkaPaths.topic_offsets(group, topic), partition)

    @staticmethod
    def broker_partitions(topic):
        return '%s/brokers/topics/%s/partitions' % (KafkaPaths.kafka_chroot, topic)

    @staticmethod
    def broker_ids():
        return '%s/brokers/ids' % (KafkaPaths.kafka_chroot)

    @staticmethod
    def broker(broker_id):
        return '%s/brokers/ids/%s' % (KafkaPaths.kafka_chroot, broker_id)

    @staticmethod
    def topics():
        return '%s/brokers/topics' % (KafkaPaths.kafka_chroot)


if __name__ == '__main__':
    report()
