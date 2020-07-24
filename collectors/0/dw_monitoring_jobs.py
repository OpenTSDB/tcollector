#!/usr/bin/env python
import sys
import time
import pymysql
import datetime
import ConfigParser
import tarfile
import yaml


SLEEP_INTERVAL = 1200   # 20 min
CONFIG_PATH = "/etc/luigi/luigi.cfg"
MONITORING_CONFIG_PATH = "tcollector/critical_tasks.yaml"
TASK_FIRST_FINISH_QUERY = """
    SELECT
        %(column)s FROM %(db)s.%(table_name)s
    WHERE
        task = "%(task)s" AND data_hour = "%(data_hour)s" AND success = "1" AND job_type = "%(job_type)s"
    ORDER BY id ASC
"""
DB = "luigi"
TABLE = "houzz_task_run_times"
DB_PORT = 3306

MONITORING_TASK_DELAY_METRIC = 'luigi.monitoring.task.delay %d %d task=%s type=%s group=%s'


def get_mysql_connection(attemps=3):
    config_parser = ConfigParser.RawConfigParser()
    config_parser.read(CONFIG_PATH)
    host = config_parser.get('core', 'worker-dirty-jobs-host')
    user = config_parser.get('core', 'worker-dirty-jobs-user')
    passwd = config_parser.get('core', 'worker-dirty-jobs-pass')
    port = DB_PORT
    db = 'luigi'
    conn = None
    attempt = 0
    while not conn:
        try:
            conn = pymysql.connect(host, user, passwd, db, port, autocommit=True)
        except pymysql.OperationalError:
            attempt += 1
            if attempt > attemps:
                raise
            else:
                time.sleep(3)
    return conn


def get_monitoring_tasks(task_config_path):
    config_parser = ConfigParser.RawConfigParser()
    config_parser.read(CONFIG_PATH)
    config_tarball_path = config_parser.get('hz_config', 'path')
    tar = tarfile.open(config_tarball_path, "r:gz")
    tcollector_config = tar.extractfile(task_config_path)
    config_dict = yaml.safe_load(tcollector_config)
    res = {k: v for d in config_dict.values() for k, v in d.items()}
    return res


def query_task_finish_time(task, data_time, job_type):
    params = {
        'column': 'run_end',
        'db': DB,
        'table_name': TABLE,
        'task': task,
        'data_hour': data_time.strftime("%Y-%m-%d %H:%M:%S"),
        'job_type': job_type
    }
    conn = get_mysql_connection()
    with conn:
        cur = conn.cursor()
        cur.execute(TASK_FIRST_FINISH_QUERY % params)
        row = cur.fetchone()
        if row:
            finish_time = row[0]
            return finish_time
    return None


def print_task_delay_metrics(task_config_path, metrics_format):
    now = datetime.datetime.now()
    tasks = get_monitoring_tasks(task_config_path)
    for task, check_params in tasks.items():
        # check details
        finished_by_hour = check_params.get('finished_by_hour', 24)
        allow_delay = check_params.get('allow_delay', 0)
        task_types = check_params.get('task_types', [])
        group = check_params.get('group', 'other')
        # calculate the data time
        cur_day = now.replace(hour=23, minute=0, second=0, microsecond=0)
        prev_day = cur_day - datetime.timedelta(days=1)
        data_time = (prev_day - datetime.timedelta(hours=allow_delay)).replace(hour=23)
        deadline = cur_day.replace(hour=finished_by_hour)
        for task_type in task_types:
            finish_time = query_task_finish_time(task, data_time, task_type)
            delay_time = None
            if finish_time:
                delay_time = time.mktime(finish_time.timetuple()) - time.mktime(deadline.timetuple())
            elif now.day != (now + datetime.timedelta(seconds=SLEEP_INTERVAL)).day:
                # it's the last check today and the task still hasn't finish
                # This means the next check will check the data_time for the next day
                # Thus the data point for this date will be missing
                # So output the current delay time as the delay
                # TODO: Revise this by storing the last data_time somewhere (e.g. redis) and increment on that
                delay_time = time.mktime(now.timetuple()) - time.mktime(deadline.timetuple())
            if delay_time:
                print(
                    metrics_format %
                    (time.mktime(data_time.timetuple()), delay_time, task, task_type, group)
                )


def main():
    while True:
        try:
            print_task_delay_metrics(MONITORING_CONFIG_PATH, MONITORING_TASK_DELAY_METRIC)
        except Exception as ex:
            print(ex)
        finally:
            sys.stdout.flush()
            time.sleep(SLEEP_INTERVAL)


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main())
