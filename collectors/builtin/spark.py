import time
import requests
from HTMLParser import HTMLParser
from collectors.lib.collectorbase import CollectorBase

SPARK_STANDALONE_MODE = 'spark_standalone_mode'
SPARK_YARN_MODE = 'spark_yarn_mode'
SPARK_MESOS_MODE = 'spark_mesos_mode'

REST_API = {
    "YARN_APPS_PATH": "ws/v1/cluster/apps",
    "SPARK_APPS_PATH": "api/v1/applications",
    "SPARK_MASTER_STATE_PATH": "json",
    "SPARK_MASTER_APP_PATH": "app",
    "MESOS_MASTER_APP_PATH": "frameworks"
}


# request :
# <track_url>/api/v1/applications/<is name , must return from 'api/v1/application' >/jobs

# spark Job Metrics
# [
#     {
#         "jobId": 0,
#         "name": "saveAsTextFile at NativeMethodAccessorImpl.java:-2",
#         "submissionTime": "2016-03-31T03:05:43.301GMT",
#         "completionTime": "2016-03-31T03:05:47.080GMT",
#         "stageIds": [
#             0,
#             1
#         ],
#         "status": "RUNNING",
#         "numTasks": 20,
#         "numActiveTasks": 30,
#         "numCompletedTasks": 40,
#         "numSkippedTasks": 50,
#         "numFailedTasks": 60,
#         "numActiveStages": 70,
#         "numCompletedStages": 80,
#         "numSkippedStages": 90,
#         "numFailedStages": 100
#     }
# ]
JOB_METRICS = [
    'numTasks',
    'numActiveTasks',
    'numCompletedTasks',
    'numSkippedTasks',
    'numFailedTasks',
    'numActiveStages',
    'numCompletedStages',
    'numSkippedStages',
    'numFailedStages'
]

# spark stage metric
# [
#     {
#         "status": "COMPLETE",
#         "stageId": 0,
#         "attemptId": 0,
#         "numActiveTasks": 100,
#         "numCompleteTasks": 101,
#         "numFailedTasks": 102,
#         "executorRunTime": 103,
#         "inputBytes": 104,
#         "inputRecords": 105,
#         "outputBytes": 106,
#         "outputRecords": 107,
#         "shuffleReadBytes": 108,
#         "shuffleReadRecords": 109,
#         "shuffleWriteBytes": 110,
#         "shuffleWriteRecords": 111,
#         "memoryBytesSpilled": 112,
#         "diskBytesSpilled": 113,
#         "name": "reduceByKey at <stdin>:1",
#         "details": "",
#         "schedulingPool": "default",
#         "accumulatorUpdates": []
#     }
# ]
STAGE_METRICS = [
    'numActiveTasks',
    'numCompleteTasks',
    'numFailedTasks',
    'executorRunTime',
    'inputBytes',
    'inputRecords',
    'outputBytes',
    'outputRecords',
    'shuffleReadBytes',
    'shuffleReadRecords',
    'shuffleWriteBytes',
    'shuffleWriteRecords',
    'memoryBytesSpilled',
    'diskBytesSpilled'
]

# spark executor metric
# [
#     {
#         "id": "driver",
#         "hostPort": "10.0.2.15:33870",
#         "rddBlocks": 99,
#         "memoryUsed": 98,
#         "diskUsed": 97,
#         "activeTasks": 96,
#         "failedTasks": 95,
#         "completedTasks": 94,
#         "totalTasks": 93,
#         "totalDuration": 92,
#         "totalInputBytes": 91,
#         "totalShuffleRead": 90,
#         "totalShuffleWrite": 89,
#         "maxMemory": 278019440,
#         "executorLogs": {}
#     }
# ]

EXECUTOR_METRICS = [
    'rddBlocks',
    'memoryUsed',
    'diskUsed',
    'activeTasks',
    'failedTasks',
    'completedTasks',
    'totalTasks',
    'totalDuration',
    'totalInputBytes',
    'totalShuffleRead',
    'totalShuffleWrite',
    'maxMemory'
]

# spacke rdd metric
# [
#     {
#         "id": 6,
#         "name": "PythonRDD",
#         "numPartitions": 2,
#         "numCachedPartitions": 2,
#         "storageLevel": "Memory Serialized 1x Replicated",
#         "memoryUsed": 284,
#         "diskUsed": 0
#     }
# ]
RDD_METRCIS = [
    'numPartitions',
    'numCachedPartitions',
    'memoryUsed',
    'diskUsed'
]


class Spark(CollectorBase):
    def __init__(self, config, logger, readq):
        super(Spark, self).__init__(config, logger, readq)

        self.spark_cluster_mode = self.get_config('spark_cluster_mode', SPARK_STANDALONE_MODE)
        self.host = self.get_config('spark_host', "localhost")
        self.port = self.get_config('spark_port', 8080)
        self.spark_url = 'http://%s:%s' % (self.host, self.port)

    def __call__(self):
        try:
            spark_apps = self._get_running_apps()
            self._spark_job_metrics(spark_apps)
            self._spark_stage_metrics(spark_apps)
            self._spark_executor_metrics(spark_apps)
            self._spark_rdd_metrics(spark_apps)
        except Exception as e:
            self.log_exception('exception collecting spark metrics %s' % e)


    def _get_running_apps(self):
        self.log_info("Choose Spark Cluster Mode : %s" % self.spark_cluster_mode)
        if self.spark_cluster_mode is None:
            raise Exception('spark_cluster_node must not be none')

        if self.spark_cluster_mode == SPARK_STANDALONE_MODE:
            return self._standalone_init()

        elif self.spark_cluster_mode == SPARK_MESOS_MODE:
            running_apps = self._mesos_init()
            return self._get_spark_app_ids(running_apps)

        elif self.spark_cluster_mode == SPARK_YARN_MODE:
            running_apps = self._yarn_init()
            return self._get_spark_app_ids(running_apps)

        else:
            raise Exception('Invalid setting for spark_cluster_mode. Received %s.' % (self.spark_cluster_mode))

    def _spark_job_metrics(self, running_apps):
        try:
            for app_id, (app_name, tracking_url) in running_apps.iteritems():
                i = 0
                ts = time.time()

                for job in self.request(url_join_4(tracking_url, REST_API["SPARK_APPS_PATH"], app_name, 'jobs')):
                    i = i + 1
                    for metric in JOB_METRICS:
                        self._readq.nput('spark.job.%s %d %d host=%s name=%s id=%s' % (metric, ts, job[metric], self.host, app_name, job['jobId']))

                self._readq.nput('spark.job.count %d %d host=%s' % (ts, i, self.host))
        except Exception as e:
            self.log_exception('exception collecting spark job metrics %s' % e)


    def _spark_stage_metrics(self, running_apps):
        try:
            for app_id, (app_name, tracking_url) in running_apps.iteritems():
                i = 0
                ts = time.time()

                for stage in self.request(url_join_4(tracking_url, REST_API["SPARK_APPS_PATH"], app_name, 'stages')):
                    i = i + 1
                    for metric in STAGE_METRICS:
                        self._readq.nput('spark.stage.%s %d %d host=%s name=%s id=%s' % (metric, ts, stage[metric], self.host, app_name, stage['stageId']))

                self._readq.nput('spark.stage.count %d %d host=%s' % (ts, i, self.host))
        except Exception as e:
            self.log_exception('exception collecting spark stage metrics %s' % e)

    def _spark_executor_metrics(self, running_apps):
        try:
            for app_id, (app_name, tracking_url) in running_apps.iteritems():
                i = 0
                ts = time.time()

                for executor in self.request(url_join_4(tracking_url, REST_API["SPARK_APPS_PATH"], app_name, 'executors')):
                    i = i + 1
                    for metric in EXECUTOR_METRICS:
                        self._readq.nput('spark.executor.%s %d %d host=%s name=%s id=%s' % (metric, ts, executor[metric], self.host, app_name, executor['id']))

                self._readq.nput('spark.executor.count %d %d host=%s' % (ts, i, self.host))
        except Exception as e:
            self.log_exception('exception collecting spark stage metrics %s' % e)

    def _spark_rdd_metrics(self, running_apps):
        try:
            for app_id, (app_name, tracking_url) in running_apps.iteritems():
                i = 0
                ts = time.time()

                for rdd in self.request(url_join_4(tracking_url, REST_API["SPARK_APPS_PATH"], app_name, 'storage/rdd')):
                    i = i + 1
                    for metric in RDD_METRCIS:
                        self._readq.nput('spark.rdd.%s %d %d host=%s name=%s id=%s' % (metric, ts, rdd[metric], self.host, app_name, rdd['id']))

                self._readq.nput('spark.rdd.count %d %d host=%s' % (ts, i, self.host))
        except Exception as e:
            self.log_exception('exception collecting spark stage metrics %s' % e)

    def _standalone_init(self):
        # Return a dictionary of {app_id: (app_name, tracking_url)} for the running Spark applications
        jdata = self.request(url_join_2(self.spark_url, REST_API['SPARK_MASTER_STATE_PATH']))
        running_apps = {}

        if jdata.get('activeapps'):
            for app in jdata['activeapps']:
                app_id = app.get('id')
                app_name = app.get('name')

                # Parse through the HTML to grab the application driver's link
                app_url = self._get_standalone_app_url(app_id)

                if app_id and app_name and app_url:
                    running_apps[app_id] = (app_name, app_url)

        return running_apps

    def _yarn_init(self):
        running_apps = {}

        metrics_json = self.request(url_join_3(self.spark_url, REST_API['YARN_APPS_PATH'], "states=RUNNING&applicationTypes=SPARK"))
        if metrics_json.get('apps'):
            if metrics_json['apps'].get('app') is not None:

                for app_json in metrics_json['apps']['app']:
                    app_id = app_json.get('id')
                    tracking_url = app_json.get('trackingUrl')
                    app_name = app_json.get('name')

                    if app_id and tracking_url and app_name:
                        running_apps[app_id] = (app_name, tracking_url)

        return running_apps

    def _mesos_init(self):
        running_apps = {}

        metrics_json = self.request(url_join_2(self.spark_url, REST_API['MESOS_MASTER_APP_PATH']))
        if metrics_json.get('frameworks'):
            for app_json in metrics_json.get('frameworks'):
                app_id = app_json.get('id')
                tracking_url = app_json.get('webui_url')
                app_name = app_json.get('name')

                if app_id and tracking_url and app_name:
                    running_apps[app_id] = (app_name, tracking_url)

        return running_apps

    def _get_standalone_app_url(self, app_id):
        '''
        Return the application URL from the app info page on the Spark master.
        Due to a bug, we need to parse the HTML manually because we cannot
        fetch JSON data from HTTP interface.
        '''

        headers = {'Accept-Encoding': 'UTF-8'}
        try:
            url = url_join_3(self.spark_url, REST_API['SPARK_MASTER_APP_PATH'], "appId=" + app_id)
            app_page = requests.get(url , headers)
            # parser html and get app_detail_direct_link
            parser = SparkParser()
            parser.feed(app_page.text)
            parser.close()
            app_detail_ui_links = parser.href

            if app_detail_ui_links and len(app_detail_ui_links) == 1:
                return parser.href
            else :
                self.log_info ("Can not get any director link form spark standalone apps pages. Maybe parser would have problem: %s" % url)

        except Exception :
            self.log_exception("Can not access SPARK_MASTER_APP_PATH %s" % url)
            raise HTTPError(app_page)

    def _get_spark_app_ids(self, running_apps):
        '''
        Because appname in yarn doesn't as same as spark. so must be transform the name;
        Traverses the Spark application master in YARN to get a Spark application ID.
        Return a dictionary of {app_id: (app_name, tracking_url)} for Spark applications
        '''
        spark_apps = {}
        for app_id, (app_name, tracking_url) in running_apps.iteritems():
            response = self.request(url_join_2(tracking_url,REST_API["SPARK_APPS_PATH"]))

            for app in response:
                app_id = app.get('id')
                app_name = app.get('name')

                if app_id and app_name:
                    spark_apps[app_id] = (app_name, tracking_url)

        return spark_apps


    def request(self, url):
        headers = {'Content-Type': 'application/json'}
        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            self.log_exception("spark collector can not access url : %s" % url)
            raise HTTPError(url)

        return resp.json()


# Get Url and Parse form Spark standalone page
# <ul class="unstyled">
#   <li><strong><a href="http://localhost:4040">Application Detail UI</a></strong></li>
# </ul>
class SparkParser(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.temp = None
        self.href = None

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for name, value in attrs:
                if name == 'href':
                    self.temp = value

    def handle_data(self, data):
        if data == 'Application Detail UI':
            self.href = self.temp

class HTTPError(RuntimeError):
    def __init__(self, resp):
        RuntimeError.__init__(self, str(resp))
        self.resp = resp


def url_join_2(prefix, uri):
    return "%s/%s" % (prefix, uri)


def url_join_3(prefix, uri, param):
    return "%s/%s?%s" % (prefix, uri, param)


def url_join_4(prefix, uri, appId, service):
    return "%s/%s/%s/%s" % (prefix, uri, appId, service)
