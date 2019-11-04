#!/usr/bin/env python
from cm_api.api_client import ApiResource, ApiException
import time
import ConfigParser
import datetime
import sys

CONFIG_PATH = "/etc/luigi/luigi.cfg"
MAP_REDUCE_TYPE = "MAPREDUCE"
SPARK_TYPE = "SPARK"
RUNNING_STATE = "RUNNING"
SUCCEEDED_STATE = "SUCCEEDED"
FAILED_STATE = "FAILED"
FINISHED_STATE = "FINISHED"
EXCEPTION_STATE = "EXCEPTION"
DURATION_METRIC = "cloudera.job.duration %d %d job_type=%s"
JOB_METRIC = "cloudera.job.headcount %d %d job_type=%s job_state=%s"
IMPALA_METRIC = "cloudera.impala.query.headcount %d %d query_state=%s"
JOB_LIMIT = 500
SLEEP_INTERVAL = 30


def collect_job_metrics():
    config_parser = ConfigParser.RawConfigParser()
    config_parser.read(CONFIG_PATH)
    host = config_parser.get("cdh", "host")
    uname = config_parser.get("cdh", "username")
    pw = config_parser.get("cdh", "password")
    ver = config_parser.get("cdh", "api-version")
    api = ApiResource(host, username=uname, password=pw, version=ver)
    clusters = api.get_all_clusters()
    cdh5 = None
    for c in clusters:
        if c.version == "CDH5":
            cdh5 = c
    services = cdh5.get_all_services()
    yarn = None
    imapla = None
    for s in services:
        if s.type == "YARN":
            yarn = s
        elif s.type == "IMPALA":
            impala = s
    from_time = datetime.datetime.fromtimestamp(time.time() - SLEEP_INTERVAL)
    to_time = datetime.datetime.fromtimestamp(time.time())
    if yarn:
        mr_apps = yarn.get_yarn_applications(start_time=from_time,
                                             end_time=to_time,
                                             filter_str="application_type=MAPREDUCE and (state=SUCCEEDED OR "
                                                        "state=FAILED "
                                                        "OR state=RUNNING)",
                                             limit=JOB_LIMIT).applications
        # spark use FINISHED state to indicate job is succeeded
        spark_apps = yarn.get_yarn_applications(start_time=from_time,
                                                end_time=to_time,
                                                filter_str="application_type=SPARK and (state=FINISHED OR "
                                                           "state=FAILED "
                                                           "OR state=RUNNING)",
                                                limit=JOB_LIMIT).applications
        mr_succeed_count, mr_failed_count, mr_running_count = 0, 0, 0
        spark_finish_count, spark_failed_count, spark_running_count = 0, 0, 0
        mr_running_dur, spark_running_dur = 0, 0
        for mr_app in mr_apps:
            if mr_app.state == RUNNING_STATE:
                mr_running_count += 1
            elif mr_app.state == SUCCEEDED_STATE:
                mr_succeed_count += 1
                mr_running_dur += (mr_app.endTime - mr_app.startTime).seconds
            elif mr_app.state == FAILED_STATE:
                mr_failed_count += 1
        for spark_app in spark_apps:
            if spark_app.state == RUNNING_STATE:
                spark_running_count += 1
            elif spark_app.state == FINISHED_STATE:
                spark_finish_count += 1
                spark_running_dur += (spark_app.endTime - spark_app.startTime).seconds
            elif spark_app.state == FAILED_STATE:
                spark_failed_count += 1
        mr_avg_dur = 0 if mr_succeed_count == 0 else round(mr_running_dur/mr_succeed_count)
        spark_avg_dur = 0 if spark_finish_count == 0 else round(spark_running_dur/spark_finish_count)
        curr_time = int(time.time() - 1)
        # Duration metrics
        print(DURATION_METRIC % (curr_time, mr_avg_dur, MAP_REDUCE_TYPE))
        print(DURATION_METRIC % (curr_time, spark_avg_dur, SPARK_TYPE))
        # Job count results
        print(JOB_METRIC % (curr_time, mr_succeed_count, MAP_REDUCE_TYPE, SUCCEEDED_STATE))
        print(JOB_METRIC % (curr_time, mr_failed_count, MAP_REDUCE_TYPE, FAILED_STATE))
        print(JOB_METRIC % (curr_time, mr_running_count, MAP_REDUCE_TYPE, RUNNING_STATE))
        print(JOB_METRIC % (curr_time, spark_finish_count, SPARK_TYPE, FINISHED_STATE))
        print(JOB_METRIC % (curr_time, spark_failed_count, SPARK_TYPE, FAILED_STATE))
        print(JOB_METRIC % (curr_time, spark_running_count, SPARK_TYPE, RUNNING_STATE))
    if impala:
        impala_queries = impala.get_impala_queries(from_time, to_time)
        run_count, finish_count, error_count = 0, 0, 0
        for query in impala_queries.queries:
            if query.queryState == RUNNING_STATE:
                run_count += 1
            elif query.queryState == FINISHED_STATE:
                finish_count += 1
            elif query.queryState == EXCEPTION_STATE:
                error_count += 1
        curr_time = int(time.time() - 1)
        print(IMPALA_METRIC % (curr_time, run_count, RUNNING_STATE))
        print(IMPALA_METRIC % (curr_time, finish_count, FINISHED_STATE))
        print(IMPALA_METRIC % (curr_time, error_count, EXCEPTION_STATE))


def main():
    while True:
        try:
            collect_job_metrics()
        except ApiException:  # ignore cloudera server issue
            pass
        finally:
            sys.stdout.flush()
            time.sleep(SLEEP_INTERVAL)


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main())
