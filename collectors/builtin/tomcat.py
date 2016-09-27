#!/usr/bin/python

import urllib2
import json
from datetime import datetime
from collectors.lib.collectorbase import CollectorBase
from collectors.lib.counter_processor import CounterPorcessor


class Tomcat(CollectorBase):
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
        super(Tomcat, self).__init__(config, logger, readq)
        self.parsers = {        # key is the mbean name
            "Catalina:name=\"http-bio-8080\",type=GlobalRequestProcessor": JolokiaGlobalRequestProcessorParser(
                self._logger),
            "java.lang:type=Memory": JolokiaMemoryParser(self._logger),
            "Catalina:name=\"http-bio-8080\",type=ThreadPool": JolokiaThreadPoolParser(self._logger),
            "java.lang:type=Threading": JolokiaThreadingParser(self._logger),
            "java.lang:name=PS Scavenge,type=GarbageCollector": JolokiaGCParser(self._logger),
            "java.lang:type=OperatingSystem": JolokiaOSParser(self._logger)
        }

    def __call__(self):
        conn = None
        try:
            self.log_info("")
            protcol = self.get_config("protocol", "http")
            port = self.get_config("port", "8080")
            url = "%(protcol)s://localhost:%(port)s/jolokia" % dict(protcol=protcol, port=port)
            req = urllib2.Request(url, Tomcat.JMX_REQUEST_JSON, {'Content-Type': 'application/json'})
            conn = urllib2.urlopen(req)
            status_code = conn.getcode()
            if status_code != 200:
                self.log_error("failed to query jolokia endpoint of tomcat. return code %d", status_code)
                return
            resp_json = conn.read()
            json_obj_or_list = json.loads(resp_json)
            if type(json_obj_or_list) is not list and json_obj_or_list['status'] != 200:
                self.log_error('failed request. status code %d, error %s', json_obj_or_list['status'],
                               json_obj_or_list['error'])
                return

            for json_dict in json_obj_or_list:
                mbean_key = json_dict["request"]["mbean"]
                try:
                    parser = self.parsers[mbean_key]
                    if parser:
                        parser.parse(json_dict, self._readq)
                    else:
                        self.log_error("failed to instantiate parser %s, skip.", mbean_key)
                except Exception as e:
                    self.log_exception("exception when parsing %s. skip", mbean_key)
        except Exception as e:
            self.log_exception("unexpected error")
        finally:
            if conn is not None:
                conn.close()


class JolokiaParserBase(object):
    def __init__(self, logger):
        self.logger = logger
        self._counter_processors = {}

    def parse(self, json_dict, readq):
        status = json_dict["status"]
        if status != 200:
            raise IOError("status code %d" % status)
        ts = json_dict["timestamp"]
        value_dict = self.metric_dict(json_dict)
        for name in self.valid_metrics():
            if name in value_dict:
                val = value_dict[name]
                if self.iscounter(name):
                    # origval = val
                    val = self._get_counter_processor(name).process_counter(ts, val)
                    # print '%s %s orig=%d, processed=%d' % (datetime.now(), name, origval, val)
                readq.nput("%s %d %d" % (self.metric_name(name), ts, val))

    def metric_dict(self, json_dict):
        return json_dict["value"]

    def valid_metrics(self):
        return []

    def metric_name(self, name):
        return "%s.%s" % ("tomcat", name)

    def iscounter(self, name):
        return False

    def _get_counter_processor(self, name):
        if not self._counter_processors.has_key(name):
            self._counter_processors[name] = CounterPorcessor()
        return self._counter_processors[name]


class JolokiaGlobalRequestProcessorParser(JolokiaParserBase):
    def __init__(self, logger):
        super(JolokiaGlobalRequestProcessorParser, self).__init__(logger)
        self.metrics = ["bytesSent", "bytesReceived", "processingTime", "errorCount", "maxTime", "requestCount"]
        self.isCounter = [True, True, False, True, False, True]

    def valid_metrics(self):
        return self.metrics

    def metric_name(self, name):
        return JolokiaParserBase.metric_name(self, "%s.%s" % ("requests", name))

    def iscounter(self, name):
        return self.isCounter[self.metrics.index(name)]


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
