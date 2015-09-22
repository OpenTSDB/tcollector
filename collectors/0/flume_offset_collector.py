#!/usr/bin/env python

from collectors.lib.kafka_offset_collector import KafkaOffsetCollector


class FlumeOffsetCollector(KafkaOffsetCollector):
    KAFKA_CHROOT = ""
    ZK_QUORUM = ["zookeeperKafkaProd-1", "zookeeperKafkaProd-2", "zookeeperKafkaProd-3", "zookeeperKafkaProd-4", "zookeeperKafkaProd-5"]
    CONSUMER_GROUP_PATTERNS = {"flume.*"}
    CLUSTER_NAME = "KafkaProd"

    def __init__(self):
        super(FlumeOffsetCollector, self).__init__()


if __name__ == "__main__":
    collector = FlumeOffsetCollector()
    collector.report()
