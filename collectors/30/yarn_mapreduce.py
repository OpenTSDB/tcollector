#!/usr/bin/env python
import socket
import time

from collectors.lib.optimizely_utils import format_tsd_key, get_json


# Constants
TIME = int(time.time())
HOST_NAME = socket.gethostname()
HOST_URL = 'http://{hostname}:8088'.format(hostname=HOST_NAME)

APP_BASE_URL = HOST_URL + '/ws/v1/cluster/apps'
JOB_BASE_URL = HOST_URL + '/proxy/{appId}/ws/v1/mapreduce/jobs'

METRIC_PREFIX = 'yarn.mapreduce'


def main():
    running_apps = get_json(APP_BASE_URL + '?state=RUNNING')

    apps = running_apps.get('apps')
    if apps is None:
        return

    for app in apps.get('app', []):
        job_url = JOB_BASE_URL.format(appId=app['id'])
        running_jobs = get_json(job_url)

        jobs = running_jobs.get('jobs')
        if jobs is None:
            continue

        for job in jobs.get('job', []):
            job_response = get_json(job_url + '/' + job['id'])
            job_detail = job_response.get('job')

            if job_detail is None:
                continue

            job_name = job_detail.get('name', "unknown").lower()

            # Skip oozie jobs
            if "oozie" in job_name:
                continue

            tags = {
                'appIdJobId': app['id'] + "_" + job['id'],
                'jobName': job_name,
            }

            for metric in ['mapsPending','mapsRunning','reducesPending','reducesRunning','newReduceAttempts',
                           'runningReduceAttempts','failedReduceAttempts','killedReduceAttempts',
                           'successfulReduceAttempts','newMapAttempts','runningMapAttempts','failedMapAttempts',
                           'killedMapAttempts','successfulMapAttempts','elapsedTime']:
                print format_tsd_key(
                    metric_key = '.'.join([METRIC_PREFIX, metric]),
                    metric_value = job_detail[metric],
                    time_ = TIME,
                    tags = tags
                )

            job_counters = get_json(job_url + '/' + job['id'] + '/counters')
            for group in job_counters['jobCounters']['counterGroup']:
                tags.update({ 'counterGroupName': group['counterGroupName'].split('.')[-1] })
                for counter in group['counter']:
                    tags.update({ 'counterName': counter['name']})
                    for metric in ['reduceCounterValue', 'mapCounterValue', 'totalCounterValue']:
                        print format_tsd_key(
                            metric_key = '.'.join([METRIC_PREFIX, metric]),
                            metric_value = counter[metric],
                            time_ = TIME,
                            tags = tags
                        )


if __name__ == '__main__':
    main()
