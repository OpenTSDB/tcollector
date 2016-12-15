#!/usr/bin/python

from collectors.lib import utils
from collectors.lib.jolokia_agent_collector_base import JolokiaAgentCollectorBase
from collectors.lib.jolokia import JolokiaParserBase
from collectors.lib.jolokia import SingleValueParser
from collectors.lib.jolokia import JolokiaG1GCParser


# https://www.datadoghq.com/blog/monitoring-kafka-performance-metrics/
class Kafka(JolokiaAgentCollectorBase):
    JMX_REQUEST_JSON = r'''[
    {
        "type": "read",
        "mbean": "kafka.server:name=UnderReplicatedPartitions,type=ReplicaManager"
    },
    {
        "type": "read",
        "mbean": "kafka.server:name=IsrShrinksPerSec,type=ReplicaManager"
    },
    {
        "type": "read",
        "mbean": "kafka.server:name=IsrExpandsPerSec,type=ReplicaManager"
    },
    {
        "type": "read",
        "mbean": "kafka.controller:name=ActiveControllerCount,type=KafkaController"
    },
    {
        "type": "read",
        "mbean": "kafka.controller:name=OfflinePartitionsCount,type=KafkaController"
    },
    {
        "type": "read",
        "mbean": "kafka.controller:name=LeaderElectionRateAndTimeMs,type=ControllerStats"
    },
    {
        "type": "read",
        "mbean": "kafka.controller:name=UncleanLeaderElectionsPerSec,type=ControllerStats"
    },
    {
        "type": "read",
        "mbean": "kafka.network:name=TotalTimeMs,request=Produce,type=RequestMetrics"
    },
    {
        "type": "read",
        "mbean": "kafka.network:name=TotalTimeMs,request=FetchConsumer,type=RequestMetrics"
    },
    {
        "type": "read",
        "mbean": "kafka.network:name=TotalTimeMs,request=FetchFollower,type=RequestMetrics"
    },
    {
        "type": "read",
        "mbean": "kafka.server:delayedOperation=Produce,name=PurgatorySize,type=DelayedOperationPurgatory"
    },
    {
        "type": "read",
        "mbean": "kafka.server:delayedOperation=Fetch,name=PurgatorySize,type=DelayedOperationPurgatory"
    },
    {
        "type": "read",
        "mbean": "kafka.server:name=BytesInPerSec,type=BrokerTopicMetrics"
    },
    {
        "type": "read",
        "mbean": "kafka.server:name=BytesOutPerSec,type=BrokerTopicMetrics"
    },
    {
        "type": "read",
        "mbean": "java.lang:name=G1 Young Generation,type=GarbageCollector"
    },
    {
        "type": "read",
        "mbean": "java.lang:name=G1 Old Generation,type=GarbageCollector"
    }
    ]'''

    CHECK_KAFKA_PID_INTERVAL = 300  # seconds, this is in case kafka restart

    def __init__(self, config, logger, readq):
        parsers = {
            "kafka.server:name=UnderReplicatedPartitions,type=ReplicaManager": URPParser(logger),
            "kafka.server:name=IsrShrinksPerSec,type=ReplicaManager": LsrPersecParser(logger, "shrink"),
            "kafka.server:name=IsrExpandsPerSec,type=ReplicaManager": LsrPersecParser(logger, "expand"),
            "kafka.controller:name=ActiveControllerCount,type=KafkaController": ActiveControllerCountParser(logger),
            "kafka.controller:name=OfflinePartitionsCount,type=KafkaController": OfflinePartitionsCountParser(logger),
            "kafka.controller:name=LeaderElectionRateAndTimeMs,type=ControllerStats": LeaderElectionParser(logger),
            "kafka.controller:name=UncleanLeaderElectionsPerSec,type=ControllerStats": UncleanLeaderElectionParser(logger),
            "kafka.network:name=TotalTimeMs,request=Produce,type=RequestMetrics": RequestTotalTimeParser(logger, "produce"),
            "kafka.network:name=TotalTimeMs,request=FetchConsumer,type=RequestMetrics": RequestTotalTimeParser(logger, "fetchconsumer"),
            "kafka.network:name=TotalTimeMs,request=FetchFollower,type=RequestMetrics": RequestTotalTimeParser(logger, "fetchfollower"),
            "kafka.server:delayedOperation=Produce,name=PurgatorySize,type=DelayedOperationPurgatory": PurgatorySizeParser(logger, "produce"),
            "kafka.server:delayedOperation=Fetch,name=PurgatorySize,type=DelayedOperationPurgatory": PurgatorySizeParser(
                logger, "fetch"),
            "kafka.server:name=BytesInPerSec,type=BrokerTopicMetrics": BytesRateParser(logger, "BytesInPerSec"),
            "kafka.server:name=BytesOutPerSec,type=BrokerTopicMetrics": BytesRateParser(logger, "BytesOutPerSec"),
            "java.lang:name=G1 Young Generation,type=GarbageCollector": JolokiaG1GCParser(logger, "kafka", "g1_yong_gen"),
            "java.lang:name=G1 Old Generation,type=GarbageCollector": JolokiaG1GCParser(logger, "kafka", "g1_old_gen")
        }
        super(Kafka, self).__init__(config, logger, readq, Kafka.JMX_REQUEST_JSON, parsers, "kafka", Kafka.CHECK_KAFKA_PID_INTERVAL)

    def __call__(self):
        super(Kafka, self).__call__()

    def cleanup(self):
        super(Kafka, self).cleanup()


class URPParser(SingleValueParser):
    def __init__(self, logger):
        super(URPParser, self).__init__(logger)

    def metric_name(self, name):
        return "kafka.server.UnderReplicatedPartitions"


class LsrPersecParser(JolokiaParserBase):
    def __init__(self, logger, atype):
        super(LsrPersecParser, self).__init__(logger)
        self.type = atype
        self.metrics = ["OneMinuteRate", "Count", "MeanRate", "FiveMinuteRate", "FifteenMinuteRate"]

    def valid_metrics(self):
        return self.metrics

    def metric_name(self, name):
        return "kafka.server.%s.%s" % (self.type, name)


class ActiveControllerCountParser(SingleValueParser):
    def __init__(self, logger):
        super(ActiveControllerCountParser, self).__init__(logger)

    def metric_name(self, name):
        return "kafka.controller.ActiveControllerCount"


class OfflinePartitionsCountParser(SingleValueParser):
    def __init__(self, logger):
        super(OfflinePartitionsCountParser, self).__init__(logger)

    def metric_name(self, name):
        return "kafka.controller.OfflinePartitionsCount"


class LeaderElectionParser(JolokiaParserBase):
    def __init__(self, logger):
        super(LeaderElectionParser, self).__init__(logger)
        self.metrics = ["OneMinuteRate", "50thPercentile", "95thPercentile", "StdDev", "Count", "999thPercentile",
                        "98thPercentile", "FiveMinuteRate", "FifteenMinuteRate", "MeanRate", "75thPercentile", "Max",
                        "Min", "Mean", "99thPercentile"]

    def valid_metrics(self):
        return self.metrics

    def metric_name(self, name):
        return "kafka.leaderelection.%s" % name


class UncleanLeaderElectionParser(JolokiaParserBase):
    def __init__(self, logger):
        super(UncleanLeaderElectionParser, self).__init__(logger)
        self.metrics = ["OneMinuteRate", "Count", "MeanRate", "FiveMinuteRate", "FifteenMinuteRate"]

    def valid_metrics(self):
        return self.metrics

    def metric_name(self, name):
        return "kafka.unclean_leaderelection.%s" % name


class RequestTotalTimeParser(JolokiaParserBase):
    def __init__(self, logger, atype):
        super(RequestTotalTimeParser, self).__init__(logger)
        self.type = atype
        self.metrics = ["50thPercentile", "Count", "StdDev", "95thPercentile", "75thPercentile", "98thPercentile", "999thPercentile", "Max", "Mean", "Min", "99thPercentile"]

    def valid_metrics(self):
        return self.metrics

    def metric_name(self, name):
        return "kafka.request.%s.%s" % (self.type, name)


class PurgatorySizeParser(SingleValueParser):
    def __init__(self, logger, atype):
        super(PurgatorySizeParser, self).__init__(logger)
        self.type = atype

    def metric_name(self, name):
        return "kafka.server.%s.PurgatorySize" % self.type


class BytesRateParser(JolokiaParserBase):
    def __init__(self, logger, atype):
        super(BytesRateParser, self).__init__(logger)
        self.type = atype
        self.metrics = ["OneMinuteRate", "Count", "MeanRate", "FiveMinuteRate", "FifteenMinuteRate"]

    def valid_metrics(self):
        return self.metrics

    def metric_name(self, name):
        return "kafka.bytesrate.%s.%s" % (self.type, name)


if __name__ == "__main__":
    inst = Kafka(None, None, utils.TestQueue())
    inst()
