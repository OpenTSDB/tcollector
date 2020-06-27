#!/usr/bin/env python
import sys
import time
import pymysql
import datetime
import ConfigParser


CONFIG_PATH = "/etc/luigi/luigi.cfg"
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

CRITICAL_TASKS_TO_TYPE_AND_ALERT_HOUR = {
    "l2.session_analytics": ("ReportTableDay", 8),
    "daily_session_processor_job": ("HadoopDailyJob", 6),
    "daily_mobile_session_processor_job": ("HadoopDailyJob", 7),
    "l2.mobile_client_event_daily": ("ImpalaTableDay", 7),
    "l2.mobile_client_event_parsed_daily": ("ImpalaTableDay", 7),
}

CRITICAL_TASK_DELAY_METRIC = 'luigi.task.delay %d %d task=%s'

BACKFILL_START = "2020-01-01"
BACKFILL_TASK_LIST = [
    "daily_mobile_session_processor_job",
    "l2.mobile_client_event_daily",
    "l2.mobile_client_event_parsed_daily"
]


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


def query_task_finish_time(task, data_time, alert_hours, job_type):
    params = {
        'column': 'run_end',
        'db': DB,
        'table_name': TABLE,
        'task': task,
        'data_hour': data_time.strftime("%Y-%m-%d %H:%M:%S"),
        'job_type': job_type
    }
    conn = get_mysql_connection()
    deadline = data_time + datetime.timedelta(hours=alert_hours)
    with conn:
        cur = conn.cursor()
        cur.execute(TASK_FIRST_FINISH_QUERY % params)
        row = cur.fetchone()
        if row:
            finish_time = row[0]
            delay_time = time.mktime(finish_time.timetuple()) - time.mktime(deadline.timetuple())
            print(
                CRITICAL_TASK_DELAY_METRIC %
                (time.mktime(data_time.timetuple()), delay_time, task)
            )


def main():
    dt_backfill_start = datetime.datetime.strptime(BACKFILL_START, '%Y-%m-%d')\
        .replace(hour=23, minute=0, second=0, microsecond=0)
    dt_backfill_end = datetime.datetime.now() - datetime.timedelta(days=1)
    try:
        check_task_data_time =dt_backfill_start
        while check_task_data_time <= dt_backfill_end:
            for task in BACKFILL_TASK_LIST:
                job_type, alert_hours = CRITICAL_TASKS_TO_TYPE_AND_ALERT_HOUR[task]
                query_task_finish_time(
                    task,
                    check_task_data_time,
                    alert_hours,
                    job_type
                )
            check_task_data_time += datetime.timedelta(days=1)
    except Exception as ex:
        print(ex)
    finally:
        sys.stdout.flush()


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main())
