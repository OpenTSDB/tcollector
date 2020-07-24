#!/usr/bin/env python
from pyhive import presto

SLEEP_INTERVAL = 900   # 15 mins
EXECUTION_QUERY = """
    SELECT 
        %(column)s
    FROM 
        %(db)s.%(table_name)s  
"""

DB = "luigi"
TABLE = """jmx.current.\"com.facebook.presto.memory:*type=ClusterMemoryPool*\" """
HOST = "presto-alpha-backend.data.houzz.net"
DB_PORT = 8086


def get_presto_connection(attemps=3):
    return presto.connect(host=host, port=DB_PORT)


def query_task_finish_time(task, alert_hours):
    columns = ["\"failedqueries.fifteenminute.count\"",
                "\"executiontime.fifteenminutes.avg\"", "\"executiontime.fifteenminutes.count\"",
                "\"executiontime.fifteenminutes.p90\"", "\"insufficientresourcesfailures.fifteenminute.count\"",
                "\"completedqueries.fifteenminute.count\"", "\"consumedinputbytes.fifteenminute.count\"",
                "\"executiontime.fifteenminutes.avg\"", "\"internalfailures.fifteenminute.count\"",
                "\"queuedqueries\"", "\"runningqueries\""]
    conn = get_presto_connection()
    with conn:
        cur = conn.cursor()
        cur.execute(TASK_FIRST_FINISH_QUERY % params)
        row = cur.fetchone()
        if row:
            data_time = row[0]
            finish_time = row[1]
            deadline = data_time + datetime.timedelta(hours=alert_hours)
            delay_time = time.mktime(finish_time.timetuple()) - time.mktime(deadline.timetuple())
            print(
                CRITICAL_TASK_DELAY_METRIC %
                (time.mktime(data_time.timetuple()), delay_time, task)
            )


def main():
    while True:
        try:
            for task, (job_type, alert_hours) in CRITICAL_TASKS_TO_TYPE_AND_ALERT_HOUR.items():
                query_task_finish_time(task, alert_hours, job_type)
        except Exception as ex:
            print(ex)
        finally:
            sys.stdout.flush()
            time.sleep(SLEEP_INTERVAL)


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main())
