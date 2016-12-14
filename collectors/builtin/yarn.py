import time
import requests
from collectors.lib import utils
from collectors.lib.collectorbase import CollectorBase

#reference by : https://github.com/DataDog/dd-agent/blob/master/checks.d/yarn.py
REST_API = {"metrics": "/ws/v1/cluster/metrics",
            "apps": "/ws/v1/cluster/apps",
            "nodes": "/ws/v1/cluster/nodes"}


# response form '/ws/v1/cluster/metrics'
# {
#     "clusterMetrics": {
#         "appsSubmitted": 0,
#         "appsCompleted": 0,
#         "appsPending": 0,
#         "appsRunning": 0,
#         "appsFailed": 0,
#         "appsKilled": 0,
#         "reservedMB": 0,
#         "availableMB": 8192,
#         "allocatedMB": 0,
#         "reservedVirtualCores": 0,
#         "availableVirtualCores": 8,
#         "allocatedVirtualCores": 0,
#         "containersAllocated": 0,
#         "containersReserved": 0,
#         "containersPending": 0,
#         "totalMB": 8192,
#         "totalVirtualCores": 8,
#         "totalNodes": 1,
#         "lostNodes": 0,
#         "unhealthyNodes": 0,
#         "decommissionedNodes": 0,
#         "rebootedNodes": 0,
#         "activeNodes": 1
#     }
# }
SUMMARY = ['appsSubmitted', 'appsCompleted', 'appsPending', 'appsRunning', 'appsFailed', 'appsKilled', 'reservedMB', 'availableMB', 'allocatedMB',
           'reservedVirtualCores', 'availableVirtualCores', 'allocatedVirtualCores', 'containersAllocated', 'containersReserved', 'containersPending',
           'totalMB', 'totalVirtualCores', 'totalNodes', 'lostNodes', 'unhealthyNodes', 'decommissionedNodes', 'rebootedNodes', 'activeNodes']

# form '/ws/v1/cluster/apps'
# {
#     "apps": {
#         "app": [
#             {
#                 "finishedTime": 1326815598530,
#                 "amContainerLogs": "http://host.domain.com:8042/node/containerlogs/container_1326815542473_0001_01_000001",
#                 "trackingUI": "History",
#                 "state": "RUNNING",
#                 "user": "user1",
#                 "id": "application_1326815542473_0001",
#                 "clusterId": 1326815542473,
#                 "finalStatus": "SUCCEEDED",
#                 "amHostHttpAddress": "host.domain.com:8042",
#                 "progress": 100,
#                 "name": "word count",
#                 "startedTime": 1326815573334,
#                 "elapsedTime": 25196,
#                 "diagnostics": "",
#                 "trackingUrl": "http://host.domain.com:8088/proxy/application_1326815542473_0001/jobhistory/job/job_1326815542473_1_1",
#                 "queue": "default",
#                 "allocatedMB": 0,
#                 "allocatedVCores": 0,
#                 "runningContainers": 0,
#                 "memorySeconds": 151730,
#                 "vcoreSeconds": 103
#             }
#         ]
#     }
# }
APPS = ['progress', 'startedTime', 'finishedTime', 'elapsedTime', 'allocatedMB', 'allocatedVCores', 'runningContainers', 'memorySeconds', 'vcoreSeconds']

# from '/ws/v1/cluster/nodes'
# {
#     "nodes": {
#         "node": [
#             {
#                 "rack": "/default-rack",
#                 "state": "NEW",
#                 "id": "h2:1235",
#                 "nodeHostName": "h2",
#                 "nodeHTTPAddress": "h2:2",
#                 "healthStatus": "Healthy",
#                 "lastHealthUpdate": 1324056895432,
#                 "healthReport": "Healthy",
#                 "numContainers": 0,
#                 "usedMemoryMB": 0,
#                 "availMemoryMB": 8192,
#                 "usedVirtualCores": 0,
#                 "availableVirtualCores": 8
#             }
#         ]
#     }
# }

NODES = ['lastHealthUpdate', 'usedMemoryMB', 'availMemoryMB', 'usedVirtualCores', 'availableVirtualCores', 'numContainers']

YARN_CLUSTER_METRICS_ELEMENT = 'clusterMetrics'

class Yarn(CollectorBase):
    def __init__(self, config, logger, readq):
        super(Yarn, self).__init__(config, logger, readq)

        self.port = self.get_config('port', 8080)
        self.host = self.get_config('host', "localhost")
        self.http_prefix = 'http://%s:%s' % (self.host, self.port)

    def __call__(self):
        try:
            self._summary_loader()
            self._apps_loader()
            self._nodes_loader()
        except Exception as e:
            self.log_exception('exception collecting yarn metrics %s' % e)

    def _summary_loader(self):
        try:
            summary = self.request(REST_API["metrics"])
            ts = time.time()
            yarn_metrics = summary[YARN_CLUSTER_METRICS_ELEMENT]
            for metric in SUMMARY:
                self._readq.nput('yarn.metrics.%s %d %d' % (metric, ts, yarn_metrics[metric]))
        except Exception as e:
            self.log_exception('exception collecting yarn metric form : %s \n %s' % ('%s%s' % (self.http_prefix, REST_API["metrics"]), e))

    def _apps_loader(self):
        try:
            metrics_json = self.request(REST_API["apps"])
            ts = time.time()

            if metrics_json:
                if metrics_json['apps'] is not None:
                    if metrics_json['apps']['app'] is not None:
                        for app_json in metrics_json['apps']['app']:
                            for metric in APPS:
                                self._readq.nput('yarn.apps.%s %d %d name=%s' % (metric, ts, app_json[metric], utils.remove_invalid_characters(app_json['name'])))

        except Exception as e:
            self.log_exception('exception collecting yarn metric form : %s \n %s' % ('%s%s' % (self.http_prefix, REST_API["apps"]), e))

    def _nodes_loader(self):
        try:
            metrics_json = self.request(REST_API["nodes"])
            ts = time.time()
            if metrics_json:
                if metrics_json['nodes'] is not None:
                    if metrics_json['nodes']['node'] is not None:
                        for node_json in metrics_json['nodes']['node']:
                            for metric in NODES:
                                self._readq.nput('yarn.nodes.%s %d %d id=%s' % (metric, ts, node_json[metric], utils.remove_invalid_characters(node_json['id'])))
        except Exception as e:
            self.log_exception('exception collecting yarn metric form : %s \n %s' % ('%s%s' % (self.http_prefix, REST_API["nodes"]), e))


    def request(self,uri):
        headers = {"Content-Type":"application/json"}
        resp = requests.get('%s%s' % (self.http_prefix, uri), headers=headers)
        if resp.status_code != 200:
            raise HTTPError(resp)

        return resp.json()



class HTTPError(RuntimeError):
    def __init__(self, resp):
        RuntimeError.__init__(self, str(resp))
        self.resp = resp
