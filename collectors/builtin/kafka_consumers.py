#!/usr/bin/python

import time
from collectors.lib.jolokia_agent_collector_base import JolokiaAgentCollectorBase
from collectors.lib.jolokia import JolokiaParserBase
from collectors.lib.collectorbase import CollectorBase


class KafkaConsumers(CollectorBase):
    def __init__(self, config, logger, readq):
        super(KafkaConsumers, self).__init__(config, logger, readq)
        self.kafka_consumer_collectors = {}
        process_port_str = self.get_config('process_names')
        if process_port_str is None:
            raise LookupError("process_names must be set in collector config file")
        process_ports = process_port_str.split(",")
        for process_port in process_ports:
            if process_port.strip() == '':
                continue
            process_port_pair = process_port.split(":")
            if len(process_port_pair) < 2 or process_port_pair[0].strip() == '' or process_port_pair[1].strip() == '':
                raise Exception("config error: %s", process_port)

            self.kafka_consumer_collectors[process_port_pair[0].strip()] = KafkaConsumer(config, logger, readq, process_port_pair[0].strip(), process_port_pair[1].strip())

    def __call__(self, *args, **kwargs):
        for process_name, kafka_consumer_collector in self.kafka_consumer_collectors.iteritems():
            try:
                self.log_info("collect for kafka consumer %s", process_name)
                self._readq.nput("kafka.consumer.state %s %s" % (int(time.time()), '0'))
                kafka_consumer_collector()
            except:
                self._readq.nput("kafka.consumer.state %s %s" % (int(time.time()), '1'))
                self.log_error("failed to kafka consumer collect for application %s", process_name)


# https://www.datadoghq.com/blog/monitoring-kafka-performance-metrics/
class KafkaConsumer(JolokiaAgentCollectorBase):
    JMX_REQUEST_JSON = r'''[
    {
        "type": "read",
        "mbean": "kafka.consumer:client-id=*,type=consumer-fetch-manager-metrics"
    }
    ]'''

    CHECK_KAFKA_CONSUMER_PID_INTERVAL = 300  # seconds, this is in case kafka restart

    def __init__(self, config, logger, readq, process_name, port):
        parsers = {"kafka.consumer:client-id=*,type=consumer-fetch-manager-metrics": KafkaConsumerFetchManagerParser(logger, process_name)}
        super(KafkaConsumer, self).__init__(config, logger, readq, KafkaConsumer.JMX_REQUEST_JSON, parsers, process_name, KafkaConsumer.CHECK_KAFKA_CONSUMER_PID_INTERVAL, port)


class KafkaConsumerFetchManagerParser(JolokiaParserBase):
    def __init__(self, logger, process_name):
        super(KafkaConsumerFetchManagerParser, self).__init__(logger)
        self.process_name = process_name
        self.metrics = ["fetch-latency-max", "fetch-size-max", "bytes-consumed-rate", "fetch-latency-avg", "fetch-throttle-time-avg", "fetch-size-avg", "records-consumed-rate", "fetch-throttle-time-max", "records-lag-max", "records-per-request-avg", "fetch-rate"]

    def parse(self, json_dict, readq, port):
        status = json_dict["status"]
        if status != 200:
            raise IOError("status code %d" % status)
        ts = json_dict["timestamp"]
        vals = json_dict["value"]
        prefix_to_remove = "kafka.consumer:"
        for mbean_name_str, val in vals.iteritems():
            # mbean_name_str => "kafka.consumer:client-id=consumer-1,type=consumer-fetch-manager-metrics"
            try:
                mbean_name_str.index(prefix_to_remove)
                mbean_name_str = mbean_name_str[len(prefix_to_remove):]
            except ValueError:
                pass

            additional_tag = "process=%s" % self.process_name
            for mbean_part in mbean_name_str.split(","):
                mbean_part_name_val_pair = mbean_part.split("=")
                if mbean_part_name_val_pair[0] == "client-id":
                    additional_tag += (" %s=%s" % (mbean_part_name_val_pair[0], mbean_part_name_val_pair[1]))
                    break

            self._process(readq, port, ts, val, additional_tag)

    def valid_metrics(self):
        return self.metrics

    def metric_name(self, name):
        return "kafka.consumer.%s" % name
