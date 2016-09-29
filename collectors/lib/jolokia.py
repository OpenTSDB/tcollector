#!/usr/bin/python

import urllib2
import json
from counter_processor import CounterPorcessor
from collectorbase import CollectorBase


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
        if name not in self._counter_processors:
            self._counter_processors[name] = CounterPorcessor()
        return self._counter_processors[name]


class JolokiaCollector(CollectorBase):
    def __init__(self, config, logger, readq, request_str, parser_map):
        super(JolokiaCollector, self).__init__(config, logger, readq)
        self.request_str = request_str
        self.parser_map = parser_map

    def __call__(self, protocol, port):
        conn = None
        try:
            url = "%(protocol)s://localhost:%(port)s/jolokia" % dict(protocol=protocol, port=port)
            req = urllib2.Request(url, self.request_str, {'Content-Type': 'application/json'})
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
                    parser = self.parser_map[mbean_key]
                    if parser:
                        parser.parse(json_dict, self._readq)
                    else:
                        self.log_error("failed to instantiate parser %s, skip.", mbean_key)
                except Exception:
                    self.log_exception("exception when parsing %s. skip", mbean_key)
        except Exception:
            self.log_exception("unexpected error")
        finally:
            if conn is not None:
                conn.close()
