#!/usr/bin/python

import subprocess
import re
import os
import time
from collectors.lib.jolokia import JolokiaCollector
from collectors.lib.jolokia import JolokiaParserBase
from collectors.lib.jolokia import SingleValueParser
from collectors.lib.jolokia import JolokiaG1GCParser


# https://www.datadoghq.com/blog/monitoring-kafka-performance-metrics/
class Kafka(JolokiaCollector):
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

    JOLOKIA_JAR = "jolokia-jvm-1.3.5-agent.jar"
    CHECK_KAFKA_PID_INTERVAL = 600  # seconds, this is in case kafka restart

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
        protocol = "http"
        self.port = Kafka.get_config(config, "port", "8778")
        super(Kafka, self).__init__(config, logger, readq, protocol, self.port, Kafka.JMX_REQUEST_JSON, parsers)
        workingdir = os.path.dirname(os.path.abspath(__file__))
        self.log_info("working dir is %s", workingdir)
        self.jolokia_file_path = os.path.join(workingdir, '../../lib', Kafka.JOLOKIA_JAR)
        if not os.path.isfile(self.jolokia_file_path):
            raise IOError("failed to find jolokia jar at %s" % self.jolokia_file_path)
        self.kafka_pattern = re.compile(r'(?P<pid>\d+) kafka', re.IGNORECASE)
        self.checkpid_time = 0
        self.kafka_pid = -1
        self.jolokia_process = None

    def __call__(self):
        curr_time = time.time()
        if curr_time - self.checkpid_time >= Kafka.CHECK_KAFKA_PID_INTERVAL:
            self.checkpid_time = curr_time
            pid, puser = self._get_kafka_pid()
            if pid is None or puser is None:
                raise Exception("failed to find kafka process, One of the (pid, puser) pair is None (%d, %s)" % (pid, puser))
            if self.kafka_pid != pid:
                self.log_info("found kafka pid %d, puser %s", pid, puser)
                if self.jolokia_process is not None:
                    self.log_info("stop jolokia agent bound to old kafka pid %d", self.kafka_pid)
                    self.stop_subprocess(self.jolokia_process, "jolokia JVM Agent")
                self.kafka_pid = pid
                self.log_info("joloia agent binds to %d", pid)
                cmdstr = "su -c \"java -jar %s --port %s start %d\" %s" % (self.jolokia_file_path, self.port, pid, puser)
                self.log_info("start jolokia agent %s", cmdstr)
                self.jolokia_process = subprocess.Popen(cmdstr, stdout=subprocess.PIPE, shell=True)
                self.log_info("jolokia process %s", self.jolokia_process.pid)
        super(Kafka, self).__call__()

    def cleanup(self):
        self.log_info('stop subprocess %d', self.jolokia_process.pid)
        self.stop_subprocess(self.jolokia_process, __name__)

    def _get_kafka_pid(self):
        # verified for both front-running and daemon type kafka process
        all_java_processes = subprocess.check_output(['jps']).split("\n")
        for pid_space_name in all_java_processes:
            m = re.search(self.kafka_pattern, pid_space_name)
            if m is not None:
                pid = m.group("pid")
                user_qstr_lines = "ps -p %s -o ruser | wc -l" % pid
                lines = int(subprocess.check_output(user_qstr_lines, shell=True).split("\n")[0])
                if lines >= 2:
                    user_qstr = "ps -p %s -o ruser | tail -n 1" % pid
                    puser = subprocess.check_output(user_qstr, shell=True).split("\n")[0]
                    return long(m.group("pid")), puser
                else:
                    return long(m.group("pid")), None
        return None, None

    @staticmethod
    def get_config(config, key, default=None, section='base'):
        if config.has_option(section, key):
            return config.get(section, key)
        else:
            return default

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
    from collectors.lib import utils

    inst = Kafka(None, None, utils.TestQueue())
    inst()
