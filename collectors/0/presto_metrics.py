#!/usr/bin/env python
from pyhive import presto
import time
import sys

SLEEP_INTERVAL = 60  # 1 mins
QUERY = """
    SELECT 
        %(column)s
    FROM 
        %(table_name)s  
"""

MEM_TABLE = """jmx.current.\"com.facebook.presto.memory:*type=ClusterMemoryPool*\" """
QUERY_TABLE = """jmx.current.\"com.facebook.presto.execution:*name=querymanager*\" """
HOST = "presto-alpha-backend.data.houzz.net"
DB_PORT = 8086

DURATION_METRIC = "presto.duration %d %d job_type=%s"
COUNT_METRIC = "presto.count %d %d job_type=%s"

MILLISECONDS_TO_SECONDS = 1000


def get_presto_connection(attemps=3):
    return presto.connect(host=HOST, port=DB_PORT)


def query_manager_time():
    columns = ["\"failedqueries.oneminute.count\"",
               "\"executiontime.oneminute.avg\"",
               "\"executiontime.oneminute.count\"",
               "\"insufficientresourcesfailures.oneminute.count\"",
               "\"completedqueries.oneminute.count\"",
               # "\"peakrunningtasksstat.fifteenminutes.avg\"",
               # "\"internalfailures.fifteenminute.count\"",
               "\"runningqueries\""]
    params = {
        'column': ', '.join(columns),
        'table_name': QUERY_TABLE,
    }
    conn = get_presto_connection()
    cur = conn.cursor()
    query = QUERY % params
    # print(query)
    cur.execute(query)
    row = cur.fetchone()
    if row:
        failed_query_count = row[0]
        execution_avg = row[1]
        execution_count = row[2]
        insufficient_count = row[3]
        completed_count = row[4]
        # peak_avg = row[5]
        running_queries = row[5]

        curr_time = int(time.time() - 1)
        # Duration metrics
        print(DURATION_METRIC % (curr_time, execution_avg//MILLISECONDS_TO_SECONDS, "Execution_Query_Duration"))
        # print(DURATION_METRIC % (curr_time, peak_avg, "Peak_Avg_Duration"))

        # Count metrics
        print(COUNT_METRIC % (curr_time, failed_query_count, "Failed_Query_Count"))
        print(COUNT_METRIC % (curr_time, execution_count, "Execution_Query_Count"))
        print(COUNT_METRIC % (curr_time, insufficient_count, "Insufficient_Resources_Query_Count"))
        print(COUNT_METRIC % (curr_time, completed_count, "Completed_Query_Count"))
        print(COUNT_METRIC % (curr_time, running_queries, "Running_Query_Count"))


def main():
    while True:
        try:
            query_manager_time()
        except Exception as ex:
            print(ex)
        finally:
            sys.stdout.flush()
            time.sleep(SLEEP_INTERVAL)


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main())
