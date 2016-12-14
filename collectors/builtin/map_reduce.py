import time
import requests
from collectors.lib.collectorbase import CollectorBase

# reference by https://hadoop.apache.org/docs/r2.7.2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapredAppMasterRest.html
REST_API = {"YARN_APPS_PATH": "ws/v1/cluster/apps",
            "MAPREDUCE_JOBS_PATH": "ws/v1/mapreduce/jobs"}

# response form 'ws/v1/mapreduce/jobs'
# {
#     "jobs": {
#         "job": [
#             {
#                 "startTime": 1453761316277,
#                 "finishTime": 0,
#                 "elapsedTime": 99221829,
#                 "id": "job_1453738555560_0001",
#                 "name": "WordCount",
#                 "user": "vagrant",
#                 "state": "RUNNING",
#                 "mapsTotal": 1,
#                 "mapsCompleted": 0,
#                 "reducesTotal": 1,
#                 "reducesCompleted": 0,
#                 "mapProgress": 48.335266,
#                 "reduceProgress": 0.0,
#                 "mapsPending": 0,
#                 "mapsRunning": 1,
#                 "reducesPending": 1,
#                 "reducesRunning": 0,
#                 "uberized": false,
#                 "diagnostics": "",
#                 "newReduceAttempts": 1,
#                 "runningReduceAttempts": 0,
#                 "failedReduceAttempts": 0,
#                 "killedReduceAttempts": 0,
#                 "successfulReduceAttempts": 0,
#                 "newMapAttempts": 0,
#                 "runningMapAttempts": 1,
#                 "failedMapAttempts": 1,
#                 "killedMapAttempts": 0,
#                 "successfulMapAttempts": 0
#             }
#         ]
#     }
# }
JOB = ['elapsedTime', 'mapsTotal', 'mapsCompleted', 'reducesTotal', 'reducesCompleted', 'mapsPending', 'mapsRunning', 'reducesPending', 'reducesRunning', 'newReduceAttempts', 'runningReduceAttempts',
       'failedReduceAttempts', 'killedReduceAttempts', 'successfulReduceAttempts', 'newMapAttempts', 'runningMapAttempts', 'failedMapAttempts', 'killedMapAttempts', 'successfulMapAttempts']

# form 'http://localhost:8088/proxy/application_1453738555560_0001/ws/v1/mapreduce/jobs/application_1453738555560_0001/counters'
# {
#     "jobCounters": {
#         "id": "job_1453738555560_0001",
#         "counterGroup": [
#             {
#                 "counterGroupName": "org.apache.hadoop.mapreduce.FileSystemCounter",
#                 "counter": [
#                     {
#                         "name": "FILE_BYTES_READ",
#                         "totalCounterValue": 0,
#                         "mapCounterValue": 1,
#                         "reduceCounterValue": 2
#                     },
#                     {
#                         "name": "FILE_BYTES_WRITTEN",
#                         "totalCounterValue": 3,
#                         "mapCounterValue": 4,
#                         "reduceCounterValue": 5
#                     }
#                 ]
#             }
#             ]
#     }
# }
JOB_COUNTER = ['reduceCounterValue', 'mapCounterValue', 'totalCounterValue']


# form 'http://localhost:8088/proxy/application_1453738555560_0001/ws/v1/mapreduce/jobs/application_1453738555560_0001/tasks'
# {
#     "tasks": {
#         "task": [
#             {
#                 "startTime": 1453761318527,
#                 "finishTime": 0,
#                 "elapsedTime": 99869037,
#                 "progress": 49.11076,
#                 "id": "task_1453738555560_0001_m_000000",
#                 "state": "RUNNING",
#                 "type": "MAP",
#                 "successfulAttempt": "",
#                 "status": "map > map"
#             }
#         ]
#     }
# }

class MapReduce(CollectorBase):
    def __init__(self, config, logger, readq):
        super(MapReduce, self).__init__(config, logger, readq)

        self.port = self.get_config('port', 8080)
        self.host = self.get_config('host', "localhost")
        self.http_prefix = 'http://%s:%s' % (self.host, self.port)

    def __call__(self):
        try:
            running_apps = self._get_running_app_ids()
            running_jobs = self._mapreduce_job_metrics(running_apps)
            self._mapreduce_job_counters_metrics(running_jobs)
            self._mapreduce_task_metrics(running_jobs)
        except Exception as e:
            self.log_exception('exception collecting mapreduce metrics %s' % e)

    def _get_running_app_ids(self):
        try:
            running_apps = {}
            metrics_json = self.request("/%s?%s" % (REST_API['YARN_APPS_PATH'], "states=RUNNING&applicationTypes=MAPREDUCE"))
            if metrics_json.get('apps'):
                if metrics_json['apps'].get('app') is not None:
                    for app_json in metrics_json['apps']['app']:
                        app_id = app_json.get('id')
                        tracking_url = app_json.get('trackingUrl')
                        app_name = app_json.get('name')

                        if app_id and tracking_url and app_name:
                            running_apps[app_id] = (app_name, tracking_url)
        except Exception as e:
            self.log_exception('exception collecting yarn apps metric for mapreduce \n %s',e)

        return running_apps


    def _mapreduce_job_metrics(self, running_apps):
        '''
        Get metrics for each MapReduce job.
        Return a dictionary for each MapReduce job
        {
          job_id: {
            'job_name': job_name,
            'app_name': app_name,
            'user_name': user_name,
            'tracking_url': tracking_url
        }
        '''
        try:
            running_jobs = {}
            for app_id, (app_name, tracking_url) in running_apps.iteritems():
                ts = time.time()
                metrics_json = self.request_url("%s%s" % (tracking_url,REST_API['MAPREDUCE_JOBS_PATH']))
                if metrics_json.get('jobs'):
                    if metrics_json['jobs'].get('job'):
                        for job_json in metrics_json['jobs']['job']:
                            job_id = job_json.get('id')
                            job_name = job_json.get('name')
                            user_name = job_json.get('user')

                            if job_id and job_name and user_name:
                                # Build the structure to hold the information for each job ID
                                running_jobs[str(job_id)] = {'job_name': str(job_name),
                                                             'app_name': str(app_name),
                                                             'user_name': str(user_name),
                                                             'tracking_url': "%s%s/%s" % (tracking_url, REST_API['MAPREDUCE_JOBS_PATH'], job_id)}

                                for metric in JOB:
                                    self._readq.nput('mapreduce.job.%s %d %d app_name=%s user_name=%s job_name=%s' % (metric, ts, job_json[metric], str(app_name), str(user_name), str(job_name)))
        except Exception as e:
            self.log_exception('exception collecting mapreduce jobs metric \n %s',e)

        return running_jobs


    def _mapreduce_job_counters_metrics(self, running_jobs):
        '''
        Get custom metrics specified for each counter
        '''
        try:
            for job_id, job_metrics in running_jobs.iteritems():
                ts = time.time()
                job_name = job_metrics['job_name']
                if job_name:
                    metrics_json = self.request_url("%s%s" % (job_metrics['tracking_url'],'/counters'))
                    if metrics_json.get('jobCounters'):
                        if metrics_json['jobCounters'].get('counterGroup'):
                            for counter_group in metrics_json['jobCounters']['counterGroup']:
                                group_name = counter_group.get('counterGroupName')
                                if group_name:
                                    if counter_group.get('counter'):
                                        for counter in counter_group['counter']:
                                            counter_name = counter.get('name')
                                            for metric in JOB_COUNTER:
                                                self._readq.nput('mapreduce.job.counter.%s %d %d app_name=%s user_name=%s job_name=%s counter_name=%s' % (metric, ts, counter[metric], job_metrics.get('app_name'), job_metrics.get('user_name'), job_name, str(counter_name).lower()))
        except Exception as e:
            self.log_exception('exception collecting mapreduce jobs counter metric \n %s',e)


    def _mapreduce_task_metrics(self, running_jobs):
            '''
            Get metrics for each MapReduce task
            Return a dictionary of {task_id: 'tracking_url'} for each MapReduce task
            '''
            try:
                for job_id, job_stats in running_jobs.iteritems():
                    ts = time.time()
                    metrics_json = self.request_url("%s%s" % (job_stats['tracking_url'],'/tasks'))
                    if metrics_json.get('tasks'):
                        if metrics_json['tasks'].get('task'):
                            for task in metrics_json['tasks']['task']:
                                task_type = task.get('type')
                                if task_type:
                                    if task_type == 'MAP':
                                        self._readq.nput('mapreduce.job.map.task.progress %d %d app_name=%s user_name=%s job_name=%s task_type=%s' % (ts, task['progress'], job_stats.get('app_name'), job_stats.get('user_name'), job_stats.get('job_name'), str(task_type).lower()))

                                    elif task_type == 'REDUCE':
                                        self._readq.nput('mapreduce.job.reduce.task.progress %d %d app_name=%s user_name=%s job_name=%s task_type=%s' % (ts, task['progress'], job_stats.get('app_name'), job_stats.get('user_name'), job_stats.get('job_name'), str(task_type).lower()))
            except Exception as e:
                self.log_exception('exception collecting mapreduce task metric \n %s',e)



    def request(self,uri):
            resp = requests.get('%s%s' % (self.http_prefix, uri))
            if resp.status_code != 200:
                raise HTTPError('%s%s' % (self.http_prefix, uri))

            return resp.json()

    def request_url(self, url):
        resp = requests.get(url)
        if resp.status_code != 200:
            if resp.status_code > 500:
                self.log_exception("mapreduce collector can not access url : %s" % url)
            raise HTTPError(url)

        return resp.json()


class HTTPError(RuntimeError):
    def __init__(self, resp):
        RuntimeError.__init__(self, str(resp))
        self.resp = resp
