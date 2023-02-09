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

QUERY_COND = """
    SELECT 
        %(column)s
    FROM 
        %(table_name)s  
    WHERE
        %(condition)s
"""

OS_STATS_QUERY = """
    SELECT 
        node, %(column)s
    FROM 
        %(table_name)s  
"""

MEM_TABLE = """jmx.current.\"trino.memory:*name=general*\" """
QUERY_TABLE = """jmx.current.\"trino.execution:*name=querymanager*\" """
OS_STATS_TABLE = """jmx.current.\"java.lang:type=operatingsystem\" """
HOST = "presto-alpha-backend.data.houzz.net"
DB_PORT = 8086

DURATION_METRIC = "presto.duration %d %d job_type=%s"
COUNT_METRIC = "presto.count %d %d job_type=%s"
MEMORY_METRIC = "presto.memory %d %d job_type=%s"
OS_STATS_METRIC = "presto.os %d %f node=%s job_type=%s"

SECONDS_TO_MILLISECONDS = 1000
GB_TO_BYTES = 1073741824


def get_presto_connection(attemps=3):
    return presto.connect(host=HOST, port=DB_PORT)


def query_os_stats(conn):
    columns = ["systemcpuload"]
    params = {
        'column': ', '.join(columns),
        'table_name': OS_STATS_TABLE,
    }
    cur = conn.cursor()
    query = OS_STATS_QUERY % params
    cur.execute(query)
    rows = cur.fetchall()
    curr_time = int(time.time() - 1)
    for row in rows:
        # add metrics here
        node = row[0]
        cpu = row[1] * 100
        print(OS_STATS_METRIC % (curr_time, cpu, node, "CPU_LOAD"))


def query_manager_time(conn):
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
        print(DURATION_METRIC % (curr_time, execution_avg//SECONDS_TO_MILLISECONDS, "Execution_Query_Duration"))
        # print(DURATION_METRIC % (curr_time, peak_avg, "Peak_Avg_Duration"))

        # Count metrics
        print(COUNT_METRIC % (curr_time, failed_query_count, "Failed_Query_Count"))
        print(COUNT_METRIC % (curr_time, execution_count, "Execution_Query_Count"))
        print(COUNT_METRIC % (curr_time, insufficient_count, "Insufficient_Resources_Query_Count"))
        print(COUNT_METRIC % (curr_time, completed_count, "Completed_Query_Count"))
        print(COUNT_METRIC % (curr_time, running_queries, "Running_Query_Count"))


# This is based on
# https://prestodb.io/blog/2019/08/19/memory-tracking#getting-visibility-into-the-memory-management-framework
def query_memory(conn):
    columns = ["blockednodes",
               "freedistributedbytes"]
    conditions = ["blockednodes >= 0"]
    params = {
        'column': ', '.join(columns),
        'table_name': MEM_TABLE,
        'condition': ' and'.join(conditions)
    }
    cur = conn.cursor()
    query = QUERY_COND % params
    # print(query)
    cur.execute(query)
    row = cur.fetchone()
    if row:
        blocked_nodes = row[0]
        general_pool_free_memory = row[1]

        curr_time = int(time.time() - 1)

        # Count metrics
        print(COUNT_METRIC % (curr_time, blocked_nodes, "Blocked_Nodes_Count"))

        # Memory metrics
        print(MEMORY_METRIC % (curr_time, general_pool_free_memory//GB_TO_BYTES, "General_Pool_Free_Memory"))


def main():
    while True:
        try:
            conn = get_presto_connection()
            query_manager_time(conn)
            query_memory(conn)
            query_os_stats(conn)
            conn.close()
        except Exception as ex:
            print(ex)
        finally:
            sys.stdout.flush()
            time.sleep(SLEEP_INTERVAL)


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main())
