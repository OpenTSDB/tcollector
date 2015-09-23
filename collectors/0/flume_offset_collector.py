#!/usr/bin/env python

import os

from collectors.lib.kafka_offset_collector import KafkaOffsetCollector

class FlumeOffsetCollector(KafkaOffsetCollector):
    KAFKA_CHROOT = ""
    ZK_QUORUM = os.getenv("KAFKA_ZOOKEEPER_QUORUM")
    CONSUMER_GROUP_PATTERNS = {"flume.*"}
    CLUSTER_NAME = os.getenv("KAFKA_CLUSTER_NAME")

    def __init__(self):
        super(FlumeOffsetCollector, self).__init__()


if __name__ == "__main__":
    collector = FlumeOffsetCollector()
    collector.report()
