#!/usr/bin/python

from collectors.lib.jolokia import JolokiaCollector
from collectors.lib.jolokia import JolokiaParserBase
from collectors.lib.collectorbase import MetricType


class Tomcat(JolokiaCollector):
    JMX_REQUEST_JSON = r'''[
    {
        "type" : "read",
        "mbean" : "Catalina:name=\"http-bio-8080\",type=GlobalRequestProcessor"
    },
    {
        "type" : "read",
        "mbean" : "Catalina:name=\"http-bio-8080\",type=ThreadPool",
        "attribute": ["connectionCount", "currentThreadCount"]
    },
    {
        "type" : "read",
        "mbean" : "java.lang:type=Memory"
    },
    {
        "type" : "read",
        "mbean" : "java.lang:type=Threading",
        "attribute": ["CurrentThreadCpuTime", "PeakThreadCount", "DaemonThreadCount", "TotalStartedThreadCount", "CurrentThreadUserTime", "ThreadCount"]
    },
    {
        "type" : "read",
        "mbean" : "java.lang:name=PS Scavenge,type=GarbageCollector",
        "attribute" : ["LastGcInfo", "CollectionCount", "CollectionTime"]
    },
    {
        "type" : "read",
        "mbean" : "java.lang:type=OperatingSystem",
        "attribute" : ["FreePhysicalMemorySize","FreeSwapSpaceSize","AvailableProcessors","ProcessCpuLoad","TotalSwapSpaceSize","ProcessCpuTime","SystemLoadAverage","OpenFileDescriptorCount","MaxFileDescriptorCount","TotalPhysicalMemorySize","CommittedVirtualMemorySize","SystemCpuLoad"]
    }
  ]'''

    def __init__(self, config, logger, readq):
        parsers = {        # key is the mbean name
            "Catalina:name=\"http-bio-8080\",type=GlobalRequestProcessor": JolokiaGlobalRequestProcessorParser(
                logger),
            "java.lang:type=Memory": JolokiaMemoryParser(logger),
            "Catalina:name=\"http-bio-8080\",type=ThreadPool": JolokiaThreadPoolParser(logger),
            "java.lang:type=Threading": JolokiaThreadingParser(logger),
            "java.lang:name=PS Scavenge,type=GarbageCollector": JolokiaGCParser(logger),
            "java.lang:type=OperatingSystem": JolokiaOSParser(logger)
        }
        super(Tomcat, self).__init__(config, logger, readq, Tomcat.JMX_REQUEST_JSON, parsers)

    def __call__(self, *arg):
        protocol = self.get_config("protocol", "http")
        port = self.get_config("port", "8080")
        super(Tomcat, self).__call__(protocol, port)


class JolokiaGlobalRequestProcessorParser(JolokiaParserBase):
    def __init__(self, logger):
        super(JolokiaGlobalRequestProcessorParser, self).__init__(logger)
        self.metrics = ["bytesSent", "bytesReceived", "processingTime", "errorCount", "maxTime", "requestCount"]
        self.type = [MetricType.COUNTER, MetricType.COUNTER, MetricType.INC, MetricType.COUNTER, MetricType.REGULAR, MetricType.COUNTER]

    def valid_metrics(self):
        return self.metrics

    def metric_name(self, name):
        return JolokiaParserBase.metric_name(self, "%s.%s" % ("requests", name))

    def get_metric_type(self, name):
        return self.type[self.metrics.index(name)]


class JolokiaMemoryParser(JolokiaParserBase):
    def __init__(self, logger):
        super(JolokiaMemoryParser, self).__init__(logger)

    def metric_dict(self, json_dict):
        nonheapmem_dict = json_dict["value"]["NonHeapMemoryUsage"]
        heapmem_dict = json_dict["value"]["HeapMemoryUsage"]
        merged_dict = {"nonheap." + key: nonheapmem_dict[key] for key in nonheapmem_dict.keys()}
        merged_dict.update({"heap." + key: heapmem_dict[key] for key in heapmem_dict.keys()})
        return merged_dict

    def valid_metrics(self):
        return ["nonheap.max", "nonheap.committed", "nonheap.init", "nonheap.used", "heap.max", "heap.committed",
                "heap.init", "heap.used"]

    def metric_name(self, name):
        return JolokiaParserBase.metric_name(self, "%s.%s" % ("memory", name))


class JolokiaThreadPoolParser(JolokiaParserBase):
    def __init__(self, logger):
        super(JolokiaThreadPoolParser, self).__init__(logger)

    def valid_metrics(self):
        return ["connectionCount", "currentThreadCount"]

    def metric_name(self, name):
        return JolokiaParserBase.metric_name(self, "%s.%s" % ("threadpool", name))


class JolokiaThreadingParser(JolokiaParserBase):
    def __init__(self, logger):
        super(JolokiaThreadingParser, self).__init__(logger)

    def valid_metrics(self):
        return ["CurrentThreadCpuTime", "PeakThreadCount", "DaemonThreadCount", "TotalStartedThreadCount", "CurrentThreadUserTime", "ThreadCount"]

    def metric_name(self, name):
        return JolokiaParserBase.metric_name(self, "%s.%s" % ("threading", name))


class JolokiaGCParser(JolokiaParserBase):
    def __init__(self, logger):
        super(JolokiaGCParser, self).__init__(logger)

    def metric_dict(self, json_dict):
        metrics_dict = {}

        survivorspace_dict = json_dict["value"]["LastGcInfo"]["memoryUsageAfterGc"]["PS Survivor Space"]
        metrics_dict.update({"survivorspace." + key: survivorspace_dict[key] for key in survivorspace_dict.keys()})

        edenspace_dict = json_dict["value"]["LastGcInfo"]["memoryUsageAfterGc"]["PS Eden Space"]
        metrics_dict.update({"edenspace." + key: edenspace_dict[key] for key in edenspace_dict.keys()})

        oldgen_dict = json_dict["value"]["LastGcInfo"]["memoryUsageAfterGc"]["PS Old Gen"]
        metrics_dict.update({"oldgen." + key: oldgen_dict[key] for key in oldgen_dict.keys()})

        codecache_dict = json_dict["value"]["LastGcInfo"]["memoryUsageAfterGc"]["Code Cache"]
        metrics_dict.update({"codecache." + key: codecache_dict[key] for key in codecache_dict.keys()})

        permgen_dict = json_dict["value"]["LastGcInfo"]["memoryUsageAfterGc"]["PS Perm Gen"]
        metrics_dict.update({"permgen." + key: permgen_dict[key] for key in permgen_dict.keys()})

        metrics_dict.update({"GcThreadCount": json_dict["value"]["LastGcInfo"]["GcThreadCount"]})
        metrics_dict.update({"CollectionCount": json_dict["value"]["CollectionCount"]})
        metrics_dict.update({"CollectionTime": json_dict["value"]["CollectionTime"]})

        return metrics_dict

    def valid_metrics(self):
        return ["GcThreadCount", "CollectionCount", "CollectionTime",
                "survivorspace.max", "survivorspace.committed", "survivorspace.init", "survivorspace.used",
                "edenspace.max", "edenspace.committed", "edenspace.init", "edenspace.used",
                "oldgen.max", "oldgen.committed", "oldgen.init", "oldgen.used",
                "codecache.max", "codecache.committed", "codecache.init", "codecache.used",
                "permgen.max", "permgen.committed", "permgen.init", "permgen.used"]

    def metric_name(self, name):
        return JolokiaParserBase.metric_name(self, "%s.%s" % ("scavenge.gc", name))


class JolokiaOSParser(JolokiaParserBase):
    def __init__(self, logger):
        super(JolokiaOSParser, self).__init__(logger)

    def valid_metrics(self):
        return ["FreePhysicalMemorySize", "FreeSwapSpaceSize", "AvailableProcessors", "ProcessCpuLoad", "TotalSwapSpaceSize",
                "ProcessCpuTime", "SystemLoadAverage", "OpenFileDescriptorCount", "MaxFileDescriptorCount", "TotalPhysicalMemorySize",
                "CommittedVirtualMemorySize", "SystemCpuLoad"]

    def metric_name(self, name):
        return JolokiaParserBase.metric_name(self, "%s.%s" % ("os", name))

if __name__ == "__main__":
    from Queue import Queue

    tomcat_inst = Tomcat(None, None, Queue())
    tomcat_inst()
