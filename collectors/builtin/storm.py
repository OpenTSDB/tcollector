import time
import requests
from collectors.lib import utils
from collectors.lib.collectorbase import CollectorBase

# reference: http://storm.apache.org/releases/1.0.2/STORM-UI-REST-API.html
REST_API = {"cluster": "/api/v1/cluster/summary",
            "supervisor": "/api/v1/supervisor/summary",
            "topology": "/api/v1/topology/summary",
            "topology_details": "/api/v1/topology/"}


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

# /api/v1/topology/summary
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

# /api/v1/topology/:id (GET)
# {
#     "name": "WordCount3",
#     "id": "WordCount3-1-1402960825",
#     "workersTotal": 3,
#     "window": "600",
#     "status": "ACTIVE",
#     "tasksTotal": 28,
#     "executorsTotal": 28,
#     "uptime": "29m 19s",
#     "uptimeSeconds": 1759,
#     "msgTimeout": 30,
#     "windowHint": "10m 0s",
#     "schedulerDisplayResource": true,
#     "topologyStats": [
#         {
#             "windowPretty": "10m 0s",
#             "window": "600",
#             "emitted": 397960,
#             "transferred": 213380,
#             "completeLatency": "0.000",
#             "acked": 213460,
#             "failed": 0
#         },
#         {
#             "windowPretty": "3h 0m 0s",
#             "window": "10800",
#             "emitted": 1190260,
#             "transferred": 638260,
#             "completeLatency": "0.000",
#             "acked": 638280,
#             "failed": 0
#         },
#         {
#             "windowPretty": "1d 0h 0m 0s",
#             "window": "86400",
#             "emitted": 1190260,
#             "transferred": 638260,
#             "completeLatency": "0.000",
#             "acked": 638280,
#             "failed": 0
#         },
#         {
#             "windowPretty": "All time",
#             "window": ":all-time",
#             "emitted": 1190260,
#             "transferred": 638260,
#             "completeLatency": "0.000",
#             "acked": 638280,
#             "failed": 0
#         }
#     ],
#     "spouts": [
#         {
#             "executors": 5,
#             "emitted": 28880,
#             "completeLatency": "0.000",
#             "transferred": 28880,
#             "acked": 0,
#             "spoutId": "spout",
#             "tasks": 5,
#             "lastError": "",
#             "errorLapsedSecs": null,
#             "failed": 0
#         }
#     ],
#     "bolts": [
#         {
#             "executors": 12,
#             "emitted": 184580,
#             "transferred": 0,
#             "acked": 184640,
#             "executeLatency": "0.048",
#             "tasks": 12,
#             "executed": 184620,
#             "processLatency": "0.043",
#             "boltId": "count",
#             "lastError": "",
#             "errorLapsedSecs": null,
#             "capacity": "0.003",
#             "failed": 0
#         },
#         {
#             "executors": 8,
#             "emitted": 184500,
#             "transferred": 184500,
#             "acked": 28820,
#             "executeLatency": "0.024",
#             "tasks": 8,
#             "executed": 28780,
#             "processLatency": "2.112",
#             "boltId": "split",
#             "lastError": "",
#             "errorLapsedSecs": null,
#             "capacity": "0.000",
#             "failed": 0
#         }
#     ],
#     "configuration": {
#         "storm.id": "WordCount3-1-1402960825",
#     },
#     "replicationCount": 1
# }

TOPOLOGY_DETAILS = {
    'topologyStats': ['emitted', 'transferred','acked', 'failed'],
    'spouts': ['executors', 'emitted', 'transferred', 'acked', 'tasks', 'failed'],
    'bolts': ['executors', 'emitted', 'transferred', 'acked', 'tasks', 'executed', 'failed']
}


class Storm(CollectorBase):
    def __init__(self, config, logger, readq):
        super(Storm, self).__init__(config, logger, readq)
        
        self.port = self.get_config('port', 8080)
        self.host = self.get_config('host', "localhost")
        self.http_prefix = 'http://%s:%s' % (self.host, self.port)

    def __call__(self):
        try:
            topology_ids = self._topology_loader()
            self._cluster_loader()
            self._supervisor_loader()
            self._topology_deatails_loader(topology_ids)
        except Exception as e:
            self.log_exception('exception collecting storm metrics %s' % e)

    def _cluster_loader(self):
        try:
            summary = self.request(REST_API["cluster"])
            ts = time.time()
            for metric in CLUSTER:
                self._readq.nput('storm.cluster.%s %d %d' % (metric, ts, summary[metric]))
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
        ids =[]
        try:
            jdata = self.request(REST_API["topology"])
            ts = time.time()
            for topology in jdata['topologies']:
                ids.append(topology['id'])
                for metric in TOPOLOGY:
                    self._readq.nput('storm.topology.%s %d %d host=%s name=%s' % (metric, ts, topology[metric], self.host, topology['name']))
        except Exception as e:
            self.log_exception('exception collecting storm topology metric form : %s \n %s' % ('%s%s' % (self.http_prefix, REST_API["supervisor"]), e))

        return ids

    def _topology_deatails_loader(self,ids):
        try:
            for id in ids:
                jdata = self.request('%s%s?%s' % (REST_API["topology_details"], id, "window=600"))
                ts = time.time()
                if jdata:
                    for topology in jdata['topologyStats']:
                        for metric in TOPOLOGY_DETAILS['topologyStats']:
                            # time window range
                            if topology['window'] == '600' :
                                self._readq.nput('storm.topology.topologyStats.%s %d %d topology_id=%s' % (metric, ts, topology[metric], utils.remove_invalid_characters(id)))

                    for spouts in jdata['spouts']:
                        for metric in TOPOLOGY_DETAILS['spouts']:
                            self._readq.nput('storm.topology.spouts.%s %d %d id=%s topology_id=%s' % (metric, ts, spouts[metric], spouts['spoutId'], utils.remove_invalid_characters(id)))

                    for bolts in jdata['bolts']:
                        for metric in TOPOLOGY_DETAILS['bolts']:
                            self._readq.nput('storm.topology.bolts.%s %d %d id=%s topology_id=%s' % (metric, ts, bolts[metric], bolts['boltId'], utils.remove_invalid_characters(id)))
        except Exception as e:
            self.log_exception('exception collecting storm topology details metric \n %s' % e)


    def request(self,uri):
        resp = requests.get('%s%s' % (self.http_prefix, uri))
        if resp.status_code != 200:
            raise HTTPError('%s%s' % (self.http_prefix, uri))

        return resp.json()



class HTTPError(RuntimeError):
    def __init__(self, resp):
        RuntimeError.__init__(self, str(resp))
        self.resp = resp
