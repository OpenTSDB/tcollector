#!/usr/bin/env python
from cm_api.api_client import ApiResource, ApiException
import time
import ConfigParser
import datetime
import sys

CONFIG_PATH = "/etc/luigi/luigi.cfg"
MAP_REDUCE_TYPE = "MAPREDUCE"
SPARK_TYPE = "SPARK"
IMPALA_TYPE = "IMPALA"
RUNNING_STATE = "RUNNING"
SUCCEEDED_STATE = "SUCCEEDED"
FAILED_STATE = "FAILED"
FINISHED_STATE = "FINISHED"
EXCEPTION_STATE = "EXCEPTION"

ALL_TYPE = "ALL"
DDL_TYPE = "DDL"
DML_TYPE = "DML"
QUERY_TYPE = "QUERY"
UNKNOWN_TYPE = "UNKNOWN"
IMPALA_ALL_QUERY_TYPES = [DDL_TYPE, DML_TYPE, QUERY_TYPE, UNKNOWN_TYPE]

HADOOP_USER = "hadoop"
TABLEAU_USER = ""
OTHER_USER = "other"
IMPALA_ALL_QUERY_USERS = [HADOOP_USER, TABLEAU_USER, OTHER_USER]
IMPALA_QUERY_USER_TO_TAG = {
    HADOOP_USER: "Pipeline",
    TABLEAU_USER: "Tableau",
    OTHER_USER: "AdHoc"
}

DURATION_METRIC = "cloudera.job.duration %d %d job_type=%s"
JOB_METRIC = "cloudera.job.headcount %d %d job_type=%s job_state=%s"

IMPALA_DURATION_METRIC = "cloudera.impala.duration %d %d query_type=%s query_state=%s cluster=%s"
IMPALA_METRIC = "cloudera.impala.query.headcount %d %d query_state=%s cluster=%s"
IMPALA_TYPE_METRIC = "cloudera.impala.query.types %d %d query_type=%s cluster=%s"
IMPALA_QUERY_USER_METRIC = "cloudera.impala.query.users %d %d query_user=%s query_type=%s cluster=%s"
JOB_LIMIT = 500
QUERY_LIMIT = 1000
SLEEP_INTERVAL = 30


def _print_mr_metrics(yarn, from_time, to_time):
    mr_apps = yarn.get_yarn_applications(start_time=from_time,
                                         end_time=to_time,
                                         filter_str="application_type=MAPREDUCE and (state=SUCCEEDED OR "
                                                    "state=FAILED "
                                                    "OR state=RUNNING)",
                                         limit=JOB_LIMIT).applications
    mr_succeed_count, mr_failed_count, mr_running_count = 0, 0, 0
    mr_total_dur = 0
    for mr_app in mr_apps:
        if mr_app.state == RUNNING_STATE:
            mr_running_count += 1
        elif mr_app.state == SUCCEEDED_STATE:
            mr_succeed_count += 1
            mr_total_dur += (mr_app.endTime - mr_app.startTime).seconds
        elif mr_app.state == FAILED_STATE:
            mr_failed_count += 1
    mr_avg_dur = 0 if mr_succeed_count == 0 else round(mr_total_dur / mr_succeed_count)
    curr_time = int(time.time() - 1)
    # Duration metrics
    print(DURATION_METRIC % (curr_time, mr_avg_dur, MAP_REDUCE_TYPE))
    # Job count results
    print(JOB_METRIC % (curr_time, mr_succeed_count, MAP_REDUCE_TYPE, SUCCEEDED_STATE))
    print(JOB_METRIC % (curr_time, mr_failed_count, MAP_REDUCE_TYPE, FAILED_STATE))
    print(JOB_METRIC % (curr_time, mr_running_count, MAP_REDUCE_TYPE, RUNNING_STATE))


def _print_spark_metrics(yarn, from_time, to_time):
    # spark use FINISHED state to indicate job is succeeded
    spark_apps = yarn.get_yarn_applications(start_time=from_time,
                                            end_time=to_time,
                                            filter_str="application_type=SPARK and (state=FINISHED OR "
                                                       "state=FAILED "
                                                       "OR state=RUNNING)",
                                            limit=JOB_LIMIT).applications
    spark_finish_count, spark_failed_count, spark_running_count = 0, 0, 0
    spark_total_dur = 0
    for spark_app in spark_apps:
        if spark_app.state == RUNNING_STATE:
            spark_running_count += 1
        elif spark_app.state == FINISHED_STATE:
            spark_finish_count += 1
            spark_total_dur += (spark_app.endTime - spark_app.startTime).seconds
        elif spark_app.state == FAILED_STATE:
            spark_failed_count += 1
    spark_avg_dur = 0 if spark_finish_count == 0 else round(spark_total_dur / spark_finish_count)
    curr_time = int(time.time() - 1)
    # Duration metrics
    print(DURATION_METRIC % (curr_time, spark_avg_dur, SPARK_TYPE))
    # Job count results
    print(JOB_METRIC % (curr_time, spark_finish_count, SPARK_TYPE, FINISHED_STATE))
    print(JOB_METRIC % (curr_time, spark_failed_count, SPARK_TYPE, FAILED_STATE))
    print(JOB_METRIC % (curr_time, spark_running_count, SPARK_TYPE, RUNNING_STATE))


def _print_impala_metrics(impala, from_time, to_time, cluster_name):
    curr_timestamp = time.time()
    impala_queries = impala.get_impala_queries(from_time, to_time, limit=QUERY_LIMIT)
    impala_run_count, impala_finish_count, impala_error_count = 0, 0, 0
    impala_finish_type_count = dict.fromkeys(IMPALA_ALL_QUERY_TYPES, 0)
    impala_finish_type_dur = dict.fromkeys(IMPALA_ALL_QUERY_TYPES, 0)
    impala_running_type_count = dict.fromkeys(IMPALA_ALL_QUERY_TYPES, 0)
    impala_running_type_dur = dict.fromkeys(IMPALA_ALL_QUERY_TYPES, 0)
    impala_total_type_count = dict.fromkeys(IMPALA_ALL_QUERY_TYPES, 0)
    impala_query_type_user_count = \
        {user: {query_type: 0 for query_type in IMPALA_ALL_QUERY_TYPES} for user in IMPALA_ALL_QUERY_USERS}
    impala_total_dur, impala_run_total_dur = 0, 0
    for query in impala_queries.queries:
        # query states
        if query.queryState == RUNNING_STATE:
            # running query duration
            if query.queryType in IMPALA_ALL_QUERY_TYPES:
                impala_running_type_count[query.queryType] += 1
                impala_running_type_dur[query.queryType] += \
                    (datetime.datetime.utcfromtimestamp(curr_timestamp) - query.startTime).seconds
            # running count and total duration
            impala_run_count += 1
            impala_run_total_dur += \
                (datetime.datetime.utcfromtimestamp(curr_timestamp) - query.startTime).seconds
        elif query.queryState == FINISHED_STATE:
            # finished query duration
            if query.queryType in IMPALA_ALL_QUERY_TYPES:
                impala_finish_type_count[query.queryType] += 1
                impala_finish_type_dur[query.queryType] += (query.endTime - query.startTime).seconds
            # finished query count and total duration
            impala_finish_count += 1
            impala_total_dur += (query.endTime - query.startTime).seconds
        elif query.queryState == EXCEPTION_STATE:
            impala_error_count += 1
        # total query types
        if query.queryType in IMPALA_ALL_QUERY_TYPES:
            impala_total_type_count[query.queryType] += 1
            # user count
            if query.user in IMPALA_ALL_QUERY_USERS:
                impala_query_type_user_count[query.user][query.queryType] += 1
            else:
                impala_query_type_user_count[OTHER_USER][query.queryType] += 1

    impala_avg_dur = 0 if impala_finish_count == 0 else round(impala_total_dur / impala_finish_count)
    impala_run_avg_dur = 0 if impala_run_count == 0 else round(impala_run_total_dur / impala_run_count)
    curr_time = int(time.time() - 1)

    # impala query type
    for query_type in IMPALA_ALL_QUERY_TYPES:
        # running query types
        print(IMPALA_TYPE_METRIC % (curr_time, impala_total_type_count[query_type], query_type, cluster_name))
        # finished query duration
        impala_finish_avg_type_dur = 0 if impala_finish_type_count[query_type] == 0 else \
            round(impala_finish_type_dur[query_type] / impala_finish_type_count[query_type])
        print(IMPALA_DURATION_METRIC % (curr_time, impala_finish_avg_type_dur, query_type, FINISHED_STATE, cluster_name))
        # running query duration
        impala_running_avg_type_dur = 0 if impala_running_type_count[query_type] == 0 else \
            round(impala_running_type_dur[query_type] / impala_running_type_count[query_type])
        print(IMPALA_DURATION_METRIC % (curr_time, impala_running_avg_type_dur, query_type, RUNNING_STATE, cluster_name))

    # print user count
    for query_user in IMPALA_ALL_QUERY_USERS:
        for query_type in IMPALA_ALL_QUERY_TYPES:
            print(IMPALA_QUERY_USER_METRIC % (
                curr_time,
                impala_query_type_user_count[query_user][query_type],
                IMPALA_QUERY_USER_TO_TAG[query_user],
                query_type, 
                cluster_name
            ))

    # total finished query duration
    print(IMPALA_DURATION_METRIC % (curr_time, impala_avg_dur, ALL_TYPE, FINISHED_STATE, cluster_name))
    # total running query duration
    print(IMPALA_DURATION_METRIC % (curr_time, impala_run_avg_dur, ALL_TYPE, RUNNING_STATE, cluster_name))
    # impala query states count
    print(IMPALA_METRIC % (curr_time, impala_run_count, RUNNING_STATE, cluster_name))
    print(IMPALA_METRIC % (curr_time, impala_finish_count, FINISHED_STATE, cluster_name))
    print(IMPALA_METRIC % (curr_time, impala_error_count, EXCEPTION_STATE, cluster_name))


def _get_cdh_api(name):
    config_parser = ConfigParser.RawConfigParser()
    config_parser.read(CONFIG_PATH)
    host = config_parser.get(name, "host")
    uname = config_parser.get(name, "username")
    pw = config_parser.get(name, "password")
    ver = config_parser.get(name, "api-version")
    api = ApiResource(host, username=uname, password=pw, version=ver)
    return api


def _get_service(api, service):
    clusters = api.get_all_clusters()
    for c in clusters:
        if c.version == "CDH5" or c.version == "CDH6":
            for s in c.get_all_services():
                if s.type == service:
                    return s
    return None


def collect_job_metrics():
    main_api = _get_cdh_api("cdh")
    yarn = _get_service(main_api, "YARN")
    impala = _get_service(main_api, "IMPALA")

    curr_timestamp = time.time()
    from_time = datetime.datetime.fromtimestamp(curr_timestamp - SLEEP_INTERVAL)
    to_time = datetime.datetime.fromtimestamp(curr_timestamp)
    if yarn:
        _print_mr_metrics(yarn, from_time, to_time)
        _print_spark_metrics(yarn, from_time, to_time)
    if impala:
        _print_impala_metrics(impala, from_time, to_time, "main")


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
