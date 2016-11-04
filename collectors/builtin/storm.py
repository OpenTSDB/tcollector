import time
import requests
from collectors.lib.collectorbase import CollectorBase

# reference: http://storm.apache.org/releases/1.0.2/STORM-UI-REST-API.html
REST_API = {"cluster": "/api/v1/cluster/summary",
            "supervisor": "/api/v1/supervisor/summary",
            "topology": "/api/v1/topology/summary"}


# response form '/api/v1/cluster/summary'
# {
#     "stormVersion": "0.9.2-incubating-SNAPSHOT",
#     "supervisors": 1,
#     "slotsTotal": 4,
#     "slotsUsed": 3,
#     "slotsFree": 1,
#     "executorsTotal": 28,
#     "tasksTotal": 28
# }
CLUSTER = ['supervisors', 'slotsTotal', 'slotsUsed', 'slotsFree', 'executorsTotal', 'tasksTotal']

# form '/api/v1/supervisor/summary'
# {
#     "supervisors": [
#         {
#             "id": "0b879808-2a26-442b-8f7d-23101e0c3696",
#             "host": "10.11.1.7",
#             "uptime": "5m 58s",
#             "uptimeSeconds": 358,
#             "slotsTotal": 4,
#             "slotsUsed": 3,
#             "totalMem": 3000,
#             "totalCpu": 400,
#             "usedMem": 1280,
#             "usedCPU": 160
#         }
#     ],
#     "schedulerDisplayResource": true
# }
SUPERVISOR = ['uptimeSeconds', 'slotsTotal', 'slotsUsed', 'totalMem', 'totalCpu', 'usedMem', 'usedCPU']

# {
#     "topologies": [
#         {
#             "id": "WordCount3-1-1402960825",
#             "name": "WordCount3",
#             "status": "ACTIVE",
#             "uptime": "6m 5s",
#             "uptimeSeconds": 365,
#             "tasksTotal": 28,
#             "workersTotal": 3,
#             "executorsTotal": 28,
#             "replicationCount": 1,
#             "requestedMemOnHeap": 640,
#             "requestedMemOffHeap": 128,
#             "requestedTotalMem": 768,
#             "requestedCpu": 80,
#             "assignedMemOnHeap": 640,
#             "assignedMemOffHeap": 128,
#             "assignedTotalMem": 768,
#             "assignedCpu": 80
#         }
#     ]
#     "schedulerDisplayResource": true
# }
TOPOLOGY = ['uptimeSeconds', 'tasksTotal', 'workersTotal', 'executorsTotal', 'replicationCount', 'requestedMemOnHeap',
            'requestedMemOffHeap', 'requestedTotalMem', 'requestedCpu', 'assignedMemOnHeap', 'assignedMemOffHeap',
            'assignedTotalMem', 'assignedCpu']


class Storm(CollectorBase):
    def __init__(self, config, logger, readq):
        super(Storm, self).__init__(config, logger, readq)
        
        self.port = self.get_config('port', 8080)
        self.host = self.get_config('host', "localhost")
        self.http_prefix = 'http://%s:%s' % (self.host, self.port)

    def __call__(self):
        try:
            self._cluster_loader()
            self._supervisor_loader()
            self._topology_loader()
        except Exception as e:
            self.log_exception('exception collecting storm metrics %s' % e)

    def _cluster_loader(self):
        try:
            summary = self.request(REST_API["cluster"])
            ts = time.time()
            for metric in CLUSTER:
                self._readq.nput('storm.cluster.%s %d %d host=%s' % (metric, ts, summary[metric], self.host))
        except Exception as e:
            self.log_exception('exception collecting storm cluster metric form : %s \n %s' % ('%s%s' % (self.http_prefix, REST_API["cluster"]), e))

    def _supervisor_loader(self):
        try:
            jdata = self.request(REST_API["supervisor"])
            ts = time.time()
            for supervisor in jdata['supervisors']:
                for metric in SUPERVISOR:
                    self._readq.nput(
                        'storm.supervisor.%s %d %d host=%s' % (metric, ts, supervisor[metric], supervisor['host']))
        except Exception as e:
            self.log_exception('exception collecting storm supervisor metric form : %s \n %s' % ('%s%s' % (self.http_prefix, REST_API["supervisor"]), e))


    def _topology_loader(self):
        try:
            jdata = self.request(REST_API["topology"])
            ts = time.time()
            for topology in jdata['topologies']:
                for metric in TOPOLOGY:
                    self._readq.nput('storm.topology.%s %d %d host=%s name=%s' % (metric, ts, topology[metric], self.host, topology['name']))
        except Exception as e:
            self.log_exception('exception collecting storm supervisor metric form : %s \n %s' % ('%s%s' % (self.http_prefix, REST_API["supervisor"]), e))


    def request(self,uri):
        resp = requests.get('%s%s' % (self.http_prefix, uri))
        if resp.status_code != 200:
            raise HTTPError(resp)

        return resp.json()



class HTTPError(RuntimeError):
    def __init__(self, resp):
        RuntimeError.__init__(self, str(resp))
        self.resp = resp
