#!/usr/bin/python
import json
import sys
import time
import urllib2
import re

TASK_LIST_URL = 'http://{host}:{port}/api/task_list?data={data}'
WORKER_LIST_URL = 'http://{host}:{port}/api/worker_list'
RESOURCE_URL = 'http://{host}:{port}/api/resources'
LUIGI_HOST = 'luigi.data.houzz.net'
LUIGI_PORT = 8082

MAX_SHOWN_TASKS = 10 ** 7
TASK_STATES = ('RUNNING', 'FAILED')
TASK_ENGINES = ('impala', 'hive', 'hadoop_job', 'spark')
TASK_ENGINES_TAG = {  # handle underscore issue with TSDB tagging
    'hadoop_job': 'hadoopJob'
}
NUM_TASKS = 'num_tasks'
PRIORITY_TAG = {
    'low': 'LS_10',
    'mid': 'BT_10_AND_99',
    'high': 'BT_100_AND_150',
    'very_high': 'GT_150'
}

TASK_STATE_METRIC = 'luigi.task.headcount %d %d task_state=%s'
RUN_TASK_COUNT_METRIC = 'luigi.task.running.count %d %d priority=%s'
RUN_TASK_DUR_METRIC = 'luigi.task.running.avgDur %d %d priority=%s'
PENDING_TASK_COUNT_METRIC = 'luigi.task.pending.count %d %d engine=%s'
PENDING_TASK_DETAIL_COUNT_METRIC = 'luigi.task.pending.detailcount %d %d engine=%s priority=%s'
WORKER_COUNT_METRIC = 'luigi.worker.headcount %d %d state=active'
WORKER_TASK_COUNT_METRIC = 'luigi.worker.taskcount %d %d state=%s'
RESOURCE_COUNT_METRIC = 'luigi.resource.count %d %d type=%s state=%s'
SLEEP_INTERVAL = 30


def fetch_data(data_params):
    url_data = urllib2.quote(json.dumps(data_params, separators=',:'))
    target_url = TASK_LIST_URL.format(host=LUIGI_HOST, port=LUIGI_PORT, data=url_data)
    return urllib2.urlopen(target_url)


def has_engine(details, task_engine):
    for key in details.get('resources').iterkeys():
        if re.match(task_engine + ".*", key):
            return True
    return False


def print_running_task():
    curr_time = int(time.time()) - 1
    data_params = {
        'status': 'RUNNING',
        'upstream_status': '',
        'max_shown_tasks': MAX_SHOWN_TASKS,
    }
    response = fetch_data(data_params)
    data = json.load(response)['response'].values()
    priority_count = {k: 0 for k in PRIORITY_TAG.keys()}
    priority_dur = {k: 0 for k in PRIORITY_TAG.keys()}  # duration of priority task in minutes
    for detail in data:
        dur = round((time.time() - detail['time_running']) / 60)  # minutes
        priority = detail['priority']
        if priority < 10:
            priority_count['low'] += 1
            priority_dur['low'] += dur
        elif 10 <= priority <= 99:
            priority_count['mid'] += 1
            priority_dur['mid'] += dur
        elif 100 <= priority <= 150:
            priority_count['high'] += 1
            priority_dur['high'] += dur
        else:
            priority_count['very_high'] += 1
            priority_dur['very_high'] += dur
    priority_avg_dur = {k: 0 if priority_count[k] == 0 else round(priority_dur[k] / priority_count[k]) for k in
                        PRIORITY_TAG.keys()}
    for k, v in PRIORITY_TAG.items():
        print(RUN_TASK_COUNT_METRIC % (curr_time, priority_count[k], v))
    for k, v in PRIORITY_TAG.items():
        print(RUN_TASK_DUR_METRIC % (curr_time, priority_avg_dur[k], v))


def print_pending_task():
    # Based on luigi source code(luigi/luigi/static/visualiser/js/visualiserApp.js),
    # pending tasks rpc call also includes upstream disabled and upstream failed, so need to filter those
    curr_time = int(time.time()) - 1
    data_params = {
        'status': 'PENDING',
        'upstream_status': '',
        'max_shown_tasks': MAX_SHOWN_TASKS,
    }
    data = json.load(fetch_data(data_params))['response'].values()
    data_params['upstream_status'] = 'UPSTREAM_DISABLED'
    disabled_data = json.load(fetch_data(data_params))['response'].values()
    data_params['upstream_status'] = 'UPSTREAM_FAILED'
    failed_data = json.load(fetch_data(data_params))['response'].values()
    filtered_tasks = set([d['display_name'] for d in data]) - \
                     set([d['display_name'] for d in disabled_data]) - \
                     set([d['display_name'] for d in failed_data])
    filtered_data = [d for d in data if d['display_name'] in filtered_tasks]
    print(TASK_STATE_METRIC % (curr_time, len(filtered_data), 'PENDING'))  # get accurate pending count
    for task_engine in TASK_ENGINES:
        task_count = sum(1 for details in filtered_data if has_engine(details, task_engine))
        print(PENDING_TASK_COUNT_METRIC % (curr_time, task_count, TASK_ENGINES_TAG.get(task_engine, task_engine)))
    for task_engine in TASK_ENGINES:
        priority_count = {k: 0 for k in PRIORITY_TAG.keys()}
        for detail in filtered_data:
            if has_engine(detail, task_engine):
                priority = detail['priority']
                if priority < 10:
                    priority_count['low'] += 1
                elif 10 <= priority <= 99:
                    priority_count['mid'] += 1
                elif 100 <= priority <= 150:
                    priority_count['high'] += 1
                else:
                    priority_count['very_high'] += 1
        for k, v in PRIORITY_TAG.items():
            print(PENDING_TASK_DETAIL_COUNT_METRIC % (
                curr_time, priority_count[k], TASK_ENGINES_TAG.get(task_engine, task_engine), v))


def print_task_count():
    curr_time = int(time.time()) - 1
    for task_state in TASK_STATES:
        # hard code max_show_tasks to 1 to only fetch number of tasks and avoid fetching task detail
        data_params = {
            'status': task_state,
            'upstream_status': '',
            'max_shown_tasks': 1,
        }
        response = fetch_data(data_params)
        data = json.load(response)['response']
        if NUM_TASKS in data:
            if data[NUM_TASKS] >= 0:
                print(TASK_STATE_METRIC % (curr_time, data[NUM_TASKS], task_state))
        else:
            print(TASK_STATE_METRIC % (curr_time, 1, task_state))


def print_worker_metric():
    curr_time = int(time.time()) - 1
    target_url = WORKER_LIST_URL.format(host=LUIGI_HOST, port=LUIGI_PORT)
    response = urllib2.urlopen(target_url)
    workers = json.load(response)['response']
    pending, running, unique = 0, 0, 0
    for worker in workers:
        pending += worker['num_pending']
        running += worker['num_running']
        unique += worker['num_uniques']
    print(WORKER_COUNT_METRIC % (curr_time, len(workers)))
    print(WORKER_TASK_COUNT_METRIC % (curr_time, running, 'num_running'))
    # print(WORKER_TASK_COUNT_METRIC % (curr_time, pending, 'num_pending'))
    # print(WORKER_TASK_COUNT_METRIC % (curr_time, unique, 'num_uniques'))


def print_resource_metric():
    curr_time = int(time.time() - 1)
    target_url = RESOURCE_URL.format(host=LUIGI_HOST, port=LUIGI_PORT)
    response = urllib2.urlopen(target_url)
    resources = json.load(response)['response']
    for key, value in resources.items():
        if key in TASK_ENGINES:
            key_tag = TASK_ENGINES_TAG.get(key, key)
            print(RESOURCE_COUNT_METRIC % (curr_time, value['total'], key_tag, 'total'))
            print(RESOURCE_COUNT_METRIC % (curr_time, value['used'], key_tag, 'used'))


def main():
    while True:
        print_task_count()
        print_running_task()
        print_pending_task()
        print_worker_metric()
        print_resource_metric()
        sys.stdout.flush()
        time.sleep(SLEEP_INTERVAL)


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main())
