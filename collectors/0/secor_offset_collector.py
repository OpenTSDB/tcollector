#!/usr/bin/env python

from collectors.lib.kafka_offset_collector import KafkaOffsetCollector


class SecorOffsetCollector(KafkaOffsetCollector):
    KAFKA_CHROOT = "/kafka"
    ZK_QUORUM = ["zookeeperHBaseProd1-1", "zookeeperHBaseProd1-2", "zookeeperHBaseProd1-3"]
    CONSUMER_GROUPS = {"secor_group"}
    SLEEP_TIME = 30
    CLUSTER_NAME = "KafkaFranz"

    def __init__(self):
        super(SecorOffsetCollector, self).__init__()


if __name__ == "__main__":
    collector = SecorOffsetCollector()
    collector.report()
