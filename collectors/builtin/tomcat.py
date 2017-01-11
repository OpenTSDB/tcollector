#!/usr/bin/python

import sys
import time
from collectors.lib import utils
from collectors.lib.jolokia import JolokiaCollector
from collectors.lib.jolokia import JolokiaParserBase
from collectors.lib.collectorbase import MetricType
from collectors.lib.collectorbase import CollectorBase


class Tomcat(CollectorBase):
    JMX_REQUEST_JSON = r'''[
    {
        "type" : "read",
        "mbean" : "Catalina:name=\"http-bio-%(port)s\",type=GlobalRequestProcessor"
    },
    {
        "type" : "read",
        "mbean" : "Catalina:name=\"http-bio-%(port)s\",type=ThreadPool",
        "attribute": ["connectionCount", "currentThreadCount", "currentThreadsBusy", "maxThreads"]
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
        "attribute" : ["FreePhysicalMemorySize","FreeSwapSpaceSize","AvailableProcessors","ProcessCpuLoad",
        "TotalSwapSpaceSize", "ProcessCpuTime", "SystemLoadAverage", "OpenFileDescriptorCount",
        "MaxFileDescriptorCount", "TotalPhysicalMemorySize", "CommittedVirtualMemorySize", "SystemCpuLoad"]
    },
    {
        "type" : "read",
        "mbean" : "Catalina:J2EEApplication=none,J2EEServer=none,WebModule=*,j2eeType=Servlet,name=*",
        "attribute": ["requestCount", "processingTime", "errorCount"]
    },
    {
        "type" : "read",
        "mbean" : "Catalina:context=*,host=*,type=Cache",
        "attribute": ["accessCount", "hitsCount"]
    },
    {
        "type": "read",
        "mbean" : "Catalina:J2EEApplication=none,J2EEServer=none,WebModule=*,name=jsp,type=JspMonitor",
        "attribute": ["jspUnloadCount", "jspCount", "jspReloadCount", "jspQueueLength"]
    }
  ]'''

    def __init__(self, config, logger, readq):
        super(Tomcat, self).__init__(config, logger, readq)
        m = sys.modules[__name__]
        parsers_template = {        # key is the mbean name
            "Catalina:name=\"http-bio-%(port)s\",type=GlobalRequestProcessor": "JolokiaGlobalRequestProcessorParser",
            "java.lang:type=Memory": "JolokiaMemoryParser",
            "Catalina:name=\"http-bio-%(port)s\",type=ThreadPool": "JolokiaThreadPoolParser",
            "java.lang:type=Threading": "JolokiaThreadingParser",
            "java.lang:name=PS Scavenge,type=GarbageCollector": "JolokiaGCParser",
            "java.lang:type=OperatingSystem": "JolokiaOSParser",
            "Catalina:J2EEApplication=none,J2EEServer=none,WebModule=*,j2eeType=Servlet,name=*": "JolokiaServletParser",
            "Catalina:context=*,host=*,type=Cache": "JolokiaCacheParser",
            "Catalina:J2EEApplication=none,J2EEServer=none,WebModule=*,name=jsp,type=JspMonitor": "JolokiaJspMonitorParser"
        }
        protocol = self.get_config("protocol", "http")
        portsStr = self.get_config("ports", "8080")
        ports = portsStr.split(",")

        self.collectors = {}
        for port in ports:
            port = port.strip()
            jmx_request_json = Tomcat.JMX_REQUEST_JSON % dict(port=port)
            parsers = {}
            for key in parsers_template:
                key_instanace = key % dict(port=port)
                parsers[key_instanace] = getattr(m, parsers_template[key])(logger)
            self.collectors[port] = JolokiaCollector(config, logger, readq, protocol, port, jmx_request_json, parsers)

    def __call__(self):
        for port in self.collectors:
            try:
                self.collectors[port].__call__()
                self._readq.nput("tomcat.state %s %s" % (int(time.time()), '0'))
            except:
                self._readq.nput("tomcat.state %s %s" % (int(time.time()), '1'))
                self.log_error("failed to collect for port %s", port)


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
        return ["connectionCount", "currentThreadCount", "currentThreadsBusy", "maxThreads"]

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


class JolokiaWebModuleParser(JolokiaParserBase):
    def __init__(self, logger):
        super(JolokiaWebModuleParser, self).__init__(logger)
        self.reserved_modules = ["jolokia", "manager", "examples", "docs", "host-manager"]

    def parse(self, json_dict, readq, port):
        status = json_dict["status"]
        if status != 200:
            raise IOError("status code %d" % status)
        ts = json_dict["timestamp"]
        vals = json_dict["value"]
        for mbean_name_str, val in vals.iteritems():   # iterate over all the values returned and apply filter
            try:
                mbean_name_str.index("Catalina:")
                mbean_name_str = mbean_name_str[len("Catalina:"):]
            except ValueError:
                pass

            additional_tag = ""
            reserved = False
            for mbean_part in mbean_name_str.split(","):    # iterate over mbean name like Catalina:J2EEApplication=none,J2EEServer=none,WebModule=*,name=jsp,type=JspMonitor
                mbean_part_name_val_pair = mbean_part.split("=")
                if mbean_part_name_val_pair[0] == "WebModule" or mbean_part_name_val_pair[0] == "context":
                    additional_tag += (" %s=%s" % (mbean_part_name_val_pair[0], utils.remove_invalid_characters(mbean_part_name_val_pair[1])))
                    for reserved_module in self.reserved_modules:
                        if mbean_part_name_val_pair[1].endswith(reserved_module):
                            reserved = True
                            break
                elif mbean_part_name_val_pair[0] == "name":
                    additional_tag += (" %s=%s" % (mbean_part_name_val_pair[0], utils.remove_invalid_characters(mbean_part_name_val_pair[1])))

            if not reserved:
                self._process(readq, port, ts, val, additional_tag)


class JolokiaServletParser(JolokiaWebModuleParser):
    def __init__(self, logger):
        super(JolokiaServletParser, self).__init__(logger)
        self.metrics = ["requestCount", "processingTime", "errorCount"]
        self.types = [MetricType.COUNTER, MetricType.REGULAR, MetricType.COUNTER]

    def valid_metrics(self):
        return self.metrics

    def metric_name(self, name):
        return JolokiaParserBase.metric_name(self, "%s.%s" % ("servlet", name))

    def get_metric_type(self, name):
        return self.types[self.metrics.index(name)]


class JolokiaCacheParser(JolokiaWebModuleParser):
    def __init__(self, logger):
        super(JolokiaCacheParser, self).__init__(logger)
        self.metrics = ["accessCount", "hitsCount"]

    def valid_metrics(self):
        return self.metrics

    def metric_name(self, name):
        return JolokiaParserBase.metric_name(self, "%s.%s" % ("cache", name))

    def get_metric_type(self, name):
        return MetricType.COUNTER


class JolokiaJspMonitorParser(JolokiaWebModuleParser):
    def __init__(self, logger):
        super(JolokiaJspMonitorParser, self).__init__(logger)
        self.metrics = ["jspUnloadCount", "jspCount", "jspReloadCount", "jspQueueLength"]
        self.type = [MetricType.COUNTER, MetricType.COUNTER, MetricType.COUNTER, MetricType.REGULAR]

    def valid_metrics(self):
        return self.metrics

    def metric_name(self, name):
        return JolokiaParserBase.metric_name(self, "%s.%s" % ("jsp", name))

    def get_metric_type(self, name):
        return self.type[self.metrics.index(name)]


if __name__ == "__main__":
    from Queue import Queue
    import ConfigParser
    import os

    try:
        collector_name = os.path.splitext(os.path.basename(__file__))[0]
        parent_dir = os.path.dirname(os.path.dirname(__file__))
        conf_file = os.path.join(parent_dir, "conf", collector_name + ".conf")
        config = ConfigParser.SafeConfigParser()
        config.read(conf_file)
        tomcat_inst = Tomcat(config, None, Queue())
        tomcat_inst()
    except Exception as e:
        print e
