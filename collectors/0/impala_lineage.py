#!/usr/bin/env python
import glob
import time
import os
import re
import sys

try:
    import json
except ImportError:
    json = None
from collectors.etc import impala_lineage_conf
from collectors.lib import utils

DEFAULT_LOG_DIR = "/var/log/impalad/lineage/"
DEFAULT_LOG_PREFIX = "impala_lineage_log"
START_TIME = "timestamp"
END_TIME = "endTime"
QUERY_TEXT = "queryText"
DURATION_METRIC = "impala.query.duration %d %d query_type=%s"  # metric timestamp duration
DEFAULT_REFRESH_INTERVAL = 180  # refresh interval to rescan latest log file
sql_commands = {'create', 'alter', 'drop', 'rename', 'truncate', 'comment', 'select', 'insert', 'update', 'delete',
                'merge', 'call', 'explain', 'lock', 'grant', 'revoke'}


def tail_file(input_file):
    """
    follow a file like tail -f
    :param input_file:
    :return:
    """
    input_file.seek(0, os.SEEK_END)
    while True:
        line = input_file.readline()
        if not line:
            time.sleep(1)
            continue
        yield line


def get_query_type(sql_str):
    """
    parse query text to extract the sql command type, otherwise return others
    :param sql_str:
    :return:
    """
    sql_str = sql_str.lower()
    # remove the special characters like /* */, --, # comments
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)
    lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]
    q = " ".join([re.split("--|#", line)[0] for line in lines])
    tokens = re.split(r"[\s();]+", q)
    for token in tokens:
        if token in sql_commands:
            return token
    return "others"


def read_impala_log():
    settings = impala_lineage_conf.get_settings()
    log_dir = settings.get("log_dir", DEFAULT_LOG_DIR)
    log_prefix = settings.get("log_prefix", DEFAULT_LOG_PREFIX)
    refresh_interval = int(settings.get("refresh_interval", DEFAULT_REFRESH_INTERVAL))
    orig_time = time.time()
    latest_log = max(filter(lambda f: log_prefix in f, glob.glob("%s*" % log_dir)), key=os.path.getctime)
    logfile = open(latest_log, "r")
    log_lines = tail_file(logfile)
    for line in log_lines:
        try:
            json_dict = json.loads(line)
            dur = int(json_dict[END_TIME]) - int(json_dict[START_TIME])
            query_type = get_query_type(str(json_dict[QUERY_TEXT]))
            time.sleep(1)  # sleep 1 second to handle tcollector log error
            print(DURATION_METRIC % (int(time.time() - 1), dur, query_type))
        except ValueError:  # ignore parsing errors
            pass
        finally:
            sys.stdout.flush()
            if abs(int(time.time() - orig_time)) > refresh_interval:  # break loop and rescan input log file
                break


def main():
    if json is None:
        utils.err("This collector requires the 'json' Python module.")
        return 13
    while True:
        read_impala_log()
        time.sleep(1)


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main())
