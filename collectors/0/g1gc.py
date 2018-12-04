#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2013  The tcollector Authors.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.  This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
# General Public License for more details.  You should have received a copy
# of the GNU Lesser General Public License along with this program.  If not,
# see <http://www.gnu.org/licenses/>.
#
# To better understand G1 GC log,
# please read Understanding G1 GC Logs (https://blogs.oracle.com/poonam/entry/understanding_g1_gc_logs)

"""
G1 GC Collector
Script that scan G1 GC log and populate OpenTSDB metrics into stdout

Metrics:

  gc.g1.concurrent_cleanup             g1 concurrent cleanup time in milliseconds
  gc.g1.concurrent_mark                g1 concurrent mark time in milliseconds
  gc.g1.concurrent_root_region_scan    g1 concurrent root region scan time in milliseconds
  gc.g1.fullgc.duration                g1 full gc duration time in milliseconds
  gc.g1.allocation                     g1 eden (young) allocation in MB
  gc.g1.promotion                      g1 promotion (young allocation - heap change) in MB
  gc.g1.heap_ratio.before              g1 heap ratio (heap size / Xmx) before GC
  gc.g1.heap_ratio.after               g1 heap ratio (heap size / Xmx) after GC

Metrics with tag "event"
  gc.g1.event.count event=young        number of young GCs happened so far
  gc.g1.event.count event=mixed        number of mixed GCs happened so far
  gc.g1.event.count event=initialmark  number of initial mark happened so far
  gc.g1.event.count event=remark       number of remark happened so far
  gc.g1.event.count event=fullgc       number of full GCs happened so far

Metrics with tag "gen":

  gc.g1.gensize gen=eden               eden heap size
  gc.g1.gensize gen=survivor           survivor heap size
  gc.g1.gensize gen=heap               total live heap size

Metrics with tag "phase":

  gc.g1.duration phase=cleanup         g1 cleanup time
  gc.g1.duration phase=parallel-time   g1 parallel time
  gc.g1.duration phase=object-copy     g1 object copy time
  gc.g1.duration phase=free-cset       g1 free cset time
  gc.g1.duration phase=ref-enq         g1 ref enq time
  gc.g1.duration phase=ref-proc        g1 ref proc time
  gc.g1.duration phase=choose-cset     g1 choose cset time
  gc.g1.duration phase=clear-ct        g1 clear ct time
  gc.g1.duration phase=scan-rs         g1 scan rset time
  gc.g1.duration phase=initial-mark    g1 initial mark time (STW)
  gc.g1.duration phase=young-pause     g1 young pause time (STW)
  gc.g1.duration phase=mixed-pause     g1 mixed pause time (STW)
  gc.g1.duration phase=remark          g1 remark time (STW)

"""

import calendar
import glob
import re
import os
import sys
import time
import traceback

from datetime import datetime, timedelta
from subprocess import Popen, PIPE

from collectors.lib import utils
from collectors.etc import g1gc_conf

GC_START_TIME_PATTERN = 1
PARALLEL_TIME_PATTERN = 2
GC_PAUSE_PATTERN = 3
GC_END_TIME_PATTERN = 4
SCAN_RS_PATTERN = 5
REMARK_PATTERN = 6
OBJECT_COPY_PATTERN = 7
ALLOCATION_PATTERN = 8
FREE_CSET_PATTERN = 9
REF_ENQ_PATTERN = 10
REF_PROC_PATTERN = 11
CHOOSE_CSET_PATTERN = 12
CLEAR_CT_PATTERN = 13

# Pattern to capture a float number, e.g. 1.23
FLOAT_NUMBER_PATTERN = '\d+(?:\.\d+)?'

pattern_map = {
    GC_START_TIME_PATTERN: re.compile('^(20\d\d)\-([01]\d)\-([0123]\d)T([012]\d):([012345]\d):([012345]\d).\d\d\d([+-]\d\d\d\d):\s*%s:\s*\[\s*(.+)' %
                                      FLOAT_NUMBER_PATTERN),
    # [Parallel Time: 157.1 ms]
    # Parallel Time is the total elapsed time spent by all the parallel GC worker threads. The following lines correspond to the parallel tasks performed by these worker threads in this total parallel time, which in this case is 157.1 ms.
    PARALLEL_TIME_PATTERN: re.compile('\s*\[Parallel Time:\s*(%s) ms,\s*GC Workers:\s*(\d+)\]\s*' %
                                      (FLOAT_NUMBER_PATTERN)),
    GC_PAUSE_PATTERN: re.compile('.* (%s)\s*secs]' % FLOAT_NUMBER_PATTERN),
    GC_END_TIME_PATTERN: re.compile('\s*\[Times: user=(%s) sys=(%s), real=(%s) secs\]\s*' %
                                    (FLOAT_NUMBER_PATTERN, FLOAT_NUMBER_PATTERN, FLOAT_NUMBER_PATTERN)),
    # [GC remark 2013-11-06T04:10:06.212-0500: 627.107: [GC ref-proc, 0.0190820 secs], 0.0500000 secs]
    #  [Times: user=0.52 sys=0.01, real=0.05 secs]
    REMARK_PATTERN: re.compile('GC remark.*\[GC ref-proc,\s*(\d+\.\d+)\s*secs\].*(\d+\.\d+)\s*secs\]$'),
    # [Scan RS (ms): Min: 0.0, Avg: 0.1, Max: 0.2, Diff: 0.2, Sum: 1.7]
    SCAN_RS_PATTERN: re.compile('\s*\[Scan RS \(ms\): Min: (%s), Avg: (%s), Max: (%s)' % (FLOAT_NUMBER_PATTERN, FLOAT_NUMBER_PATTERN, FLOAT_NUMBER_PATTERN)),
    OBJECT_COPY_PATTERN: re.compile('\s*\[Object Copy \(ms\): Min: (%s), Avg: (%s), Max: (%s)' % (FLOAT_NUMBER_PATTERN, FLOAT_NUMBER_PATTERN, FLOAT_NUMBER_PATTERN)),
    # [Eden: 3584.0M(3584.0M)->0.0B(3584.0M) Survivors: 512.0M->512.0M Heap: 91.2G(100.8G)->87.9G(100.8G)]
    ALLOCATION_PATTERN: re.compile('^\s*\[Eden: (%s)([BMG])\(\d+(?:\.\d+)?[MG]\)\->(%s)([BMG])\((%s)([BMG])\) Survivors: (%s)([BMG])\->(%s)([BMG]).+Heap: (%s)G\((%s)G\)\->(%s)G\((%s)G\).+' % (FLOAT_NUMBER_PATTERN, FLOAT_NUMBER_PATTERN, FLOAT_NUMBER_PATTERN, FLOAT_NUMBER_PATTERN, FLOAT_NUMBER_PATTERN, FLOAT_NUMBER_PATTERN, FLOAT_NUMBER_PATTERN, FLOAT_NUMBER_PATTERN, FLOAT_NUMBER_PATTERN)),
    # [Free CSet: 1.1 ms]
    # Time spent in freeing the collection set data structure.
    FREE_CSET_PATTERN: re.compile('^\s*\[Free CSet: (%s) ms\]$' % FLOAT_NUMBER_PATTERN),
    # [Ref Enq: 0.3 ms]
    # Time spent in enqueuing references to the ReferenceQueues.
    REF_ENQ_PATTERN: re.compile('\s*\[Ref Enq: (%s)' % FLOAT_NUMBER_PATTERN),
    # [Ref Proc: 0.3 ms]
    # Total time spent in processing Reference objects.
    REF_PROC_PATTERN: re.compile('\s*\[Ref Proc: (%s)' % FLOAT_NUMBER_PATTERN),
    # [Choose CSet: 0.0 ms]
    # Time spent in selecting the regions for the Collection Set.
    CHOOSE_CSET_PATTERN: re.compile('\s*\[Choose CSet: (%s)' % FLOAT_NUMBER_PATTERN),
    # [Clear CT: 0.1 ms]
    # This is the time spent in clearing the Card Table. This task is performed in serial mode.
    CLEAR_CT_PATTERN: re.compile('\s*\[Clear CT: (%s)' % FLOAT_NUMBER_PATTERN),

}

# Utilities
def get_file_end(file_handler):
    file_handler.seek(0, 2)
    return file_handler.tell()

def get_latest_gc_log(log_dir, log_name_pattern):
    sorted_gc_logs = sorted(glob.glob(os.path.join(log_dir, log_name_pattern)))
    if len(sorted_gc_logs) == 0:
        raise Exception('Did not find any gc logs under folder: "' +
                        log_dir + '" with pattern: "' + log_name_pattern + '"')
    return sorted_gc_logs[-1]

def true_unix_timestamp(year, month, day, hour, minute, second, timezone):
    d = datetime(year, month, day, hour, minute, second) - timedelta(seconds=36 * timezone)
    return calendar.timegm(d.utctimetuple())

def to_size_in_mb(data_size, unit):
    '''Convert size in given unit: GB or B to size in MB '''
    if unit == 'G': return data_size * 1024
    elif unit == 'B': return data_size / (1024 * 1024.0)
    else: return data_size

def match_pattern(line):
    for pattern_name, pattern in pattern_map.items():
        m = pattern.match(line)
        if m: return (pattern_name, m)
    return (None, None)

def sec2milli(seconds):
    return 1000 * seconds

def flush_collector(collector):
    for metric_name, value in collector['data'].items():
        print(metric_name % (collector['timestamp'], value))

    collector['timestamp'] = None
    collector['data'] = {}

def collect_metric(metric_name, timestamp, value, collector):
    if collector['timestamp'] != timestamp:
        flush_collector(collector)

    collector['timestamp'] = timestamp
    collector['data'][metric_name] = collector['data'].get(metric_name, 0) + value

def collect_metric_with_prefix(prefix, metric_name, timestamp, value, collector):
    new_metric_name = metric_name
    p = '' if prefix is None else prefix.strip()
    if len(p) > 0:
        new_metric_name = '.'.join([p, metric_name])
    collect_metric(new_metric_name, timestamp, value, collector)

def unmatched_gc_log(line): pass

# Simple gc events, don't have inner gc events
def concurrent_cleanup_handler(prefix, log_line, timestamp, collector, file_handler):
    concurrent_clean_up_time = sec2milli(float(pattern_map[GC_PAUSE_PATTERN].match(log_line).group(1)))
    collect_metric_with_prefix(prefix, "gc.g1.concurrent_cleanup %s %s", timestamp, concurrent_clean_up_time, collector)

def concurrent_mark_handler(prefix, log_line, timestamp, collector, file_handler):
    concurrent_mark_time = sec2milli(float(pattern_map[GC_PAUSE_PATTERN].match(log_line).group(1)))
    collect_metric_with_prefix(prefix, "gc.g1.concurrent_mark %s %s", timestamp, concurrent_mark_time, collector)

def concurrent_root_region_scan_handler(prefix, log_line, timestamp, collector, file_handler):
    concurrent_root_region_scan_time = sec2milli(float(pattern_map[GC_PAUSE_PATTERN].match(log_line).group(1)))
    collect_metric_with_prefix(prefix, "gc.g1.concurrent_root_region_scan %s %s", timestamp, concurrent_root_region_scan_time, collector)

def cleanup_handler(prefix, log_line, timestamp, collector, file_handler):
    clean_up_time = sec2milli(float(pattern_map[GC_PAUSE_PATTERN].match(log_line).group(1)))
    collect_metric_with_prefix(prefix, "gc.g1.duration %s %s phase=cleanup", timestamp, clean_up_time, collector)

def fullgc_handler(prefix, log_line, timestamp, collector, file_handler):
    full_gc_time = sec2milli(float(pattern_map[GC_PAUSE_PATTERN].match(log_line).group(1)))
    collect_metric_with_prefix(prefix, "gc.g1.fullgc.duration %s %s", timestamp, full_gc_time, collector)

# Inner gc events, which we should have a matcher object
def parallel_time_handler(prefix, matcher, timestamp, collector, file_handler):
    parallel_time, num_of_gc_workers = float(matcher.group(1)), float(matcher.group(2))
    collect_metric_with_prefix(prefix, "gc.g1.duration %s %s phase=parallel-time", timestamp, parallel_time, collector)

def object_copy_handler(prefix, matcher, timestamp, collector, file_handler):
    min_time, avg_time, max_time = [float(matcher.group(i)) for i in range(1, 4)]
    collect_metric_with_prefix(prefix, "gc.g1.duration %s %s phase=object-copy", timestamp, avg_time, collector)

def allocation_handler(prefix, matcher, timestamp, collector, file_handler):
    eden_before_in_size, eden_after_in_size = matcher.group(2), matcher.group(4)
    eden_before = to_size_in_mb(float(matcher.group(1)), eden_before_in_size)
    eden_after = to_size_in_mb(float(matcher.group(3)), eden_after_in_size)
    eden_capacity_after = to_size_in_mb(float(matcher.group(5)), matcher.group(6))

    survivor_before_in_size, survivor_after_in_size = matcher.group(8), matcher.group(10)
    survivor_before = to_size_in_mb(float(matcher.group(7)), survivor_before_in_size)
    survivor_after = to_size_in_mb(float(matcher.group(9)), survivor_after_in_size)

    heap_before = float(matcher.group(11))
    heap_total_size_before = float(matcher.group(12))
    heap_after = float(matcher.group(13))
    heap_total_size_after = float(matcher.group(14))
    heap_after_in_mb = to_size_in_mb(heap_after, 'G')

    collect_metric_with_prefix(prefix, "gc.g1.allocation %s %s", timestamp, eden_before - eden_after, collector)
    collect_metric_with_prefix(prefix, "gc.g1.promotion %s %s", timestamp, (eden_before - eden_after) - (heap_before - heap_after), collector)
    collect_metric_with_prefix(prefix, "gc.g1.heap_ratio.before %s %s", timestamp, heap_before / heap_total_size_before, collector)
    collect_metric_with_prefix(prefix, "gc.g1.heap_ratio.after %s %s", timestamp, heap_after / heap_total_size_after, collector)
    collector['gensize']['eden'] = eden_capacity_after
    collector['gensize']['survivor'] = survivor_after
    collector['gensize']['heap'] = heap_after_in_mb

def free_cset_handler(prefix, matcher, timestamp, collector, file_handler):
    free_cset_time = float(matcher.group(1))
    collect_metric_with_prefix(prefix, "gc.g1.duration %s %s phase=free-cset", timestamp, free_cset_time, collector)

def ref_enq_handler(prefix, matcher, timestamp, collector, file_handler):
    ref_enq_time = float(matcher.group(1))
    collect_metric_with_prefix(prefix, "gc.g1.duration %s %s phase=ref-enq", timestamp, ref_enq_time, collector)

def ref_proc_handler(prefix, matcher, timestamp, collector, file_handler):
    ref_proc_time = float(matcher.group(1))
    collect_metric_with_prefix(prefix, "gc.g1.duration %s %s phase=ref-proc", timestamp, ref_proc_time, collector)

def choose_cset_handler(prefix, matcher, timestamp, collector, file_handler):
    choose_cset_time = float(matcher.group(1))
    collect_metric_with_prefix(prefix, "gc.g1.duration %s %s phase=choose-cset", timestamp, choose_cset_time, collector)

def clear_ct_handler(prefix, matcher, timestamp, collector, file_handler):
    clear_ct_time = float(matcher.group(1))
    collect_metric_with_prefix(prefix, "gc.g1.duration %s %s phase=clear-ct", timestamp, clear_ct_time, collector)

def scan_rs_handler(prefix, matcher, timestamp, collector, file_handler):
    min_time, avg_time, max_time = [float(matcher.group(i)) for i in range(1, 4)]
    collect_metric_with_prefix(prefix, "gc.g1.duration %s %s phase=scan-rs", timestamp, avg_time, collector)

# Complex GC events: initial-mark, young-pause, mixed-pause and remark
# These GC events contains several inner gc events and we must call match_remaining_log to parse remaining gc events
def initial_mark_handler(prefix, log_line, timestamp, collector, file_handler):
    m = pattern_map[GC_PAUSE_PATTERN].match(log_line)
    initial_mark_pause_time = sec2milli(float(m.group(1)))
    collect_metric_with_prefix(prefix, "gc.g1.duration %s %s phase=initial-mark", timestamp, initial_mark_pause_time, collector)
    match_remaining_log(prefix, timestamp, collector, file_handler)

def young_pause_handler(prefix, log_line, timestamp, collector, file_handler):
    m = pattern_map[GC_PAUSE_PATTERN].match(log_line)
    young_pause_time = sec2milli(float(m.group(1)))
    collect_metric_with_prefix(prefix, "gc.g1.duration %s %s phase=young-pause", timestamp, young_pause_time, collector)
    match_remaining_log(prefix, timestamp, collector, file_handler)

def mixed_pause_handler(prefix, log_line, timestamp, collector, file_handler):
    m = pattern_map[GC_PAUSE_PATTERN].match(log_line)
    mixed_pause_time = sec2milli(float(m.group(1)))
    collect_metric_with_prefix(prefix, "gc.g1.duration %s %s phase=mixed-pause", timestamp, mixed_pause_time, collector)
    match_remaining_log(prefix, timestamp, collector, file_handler)

def remark_handler(prefix, log_line, timestamp, collector, file_handler):
    m =  pattern_map[REMARK_PATTERN].match(log_line)
    ref_process_time, remark_time = [sec2milli(float(m.group(i))) for i in range(1, 3)]
    collect_metric_with_prefix(prefix, "gc.g1.duration %s %s phase=remark", timestamp, remark_time, collector)
    match_remaining_log(prefix, timestamp, collector, file_handler)

def match_remaining_log(prefix, timestamp, collector, file_handler):
    while True:
        line = file_handler.readline()
        if len(line) == 0: break
        pattern_name, matcher = match_pattern(line)

        if pattern_name == GC_END_TIME_PATTERN: break
        elif pattern_name == PARALLEL_TIME_PATTERN: parallel_time_handler(prefix, matcher, timestamp, collector, file_handler)
        elif pattern_name == SCAN_RS_PATTERN: scan_rs_handler(prefix, matcher, timestamp, collector, file_handler)
        elif pattern_name == OBJECT_COPY_PATTERN: object_copy_handler(prefix, matcher, timestamp, collector, file_handler)
        elif pattern_name == ALLOCATION_PATTERN: allocation_handler(prefix, matcher, timestamp, collector, file_handler)
        elif pattern_name == FREE_CSET_PATTERN: free_cset_handler(prefix, matcher, timestamp, collector, file_handler)
        elif pattern_name == REF_ENQ_PATTERN: ref_enq_handler(prefix, matcher, timestamp, collector, file_handler)
        elif pattern_name == REF_PROC_PATTERN: ref_proc_handler(prefix, matcher, timestamp, collector, file_handler)
        elif pattern_name == CHOOSE_CSET_PATTERN: choose_cset_handler(prefix, matcher, timestamp, collector, file_handler)
        elif pattern_name == CLEAR_CT_PATTERN: clear_ct_handler(prefix, matcher, timestamp, collector, file_handler)
        else: unmatched_gc_log(line)

def isPause(type, cause):
    return 'GC pause' in cause and type in cause

def process_gc_record(prefix, file_handler, timestamp, cause, collector):
    # process simple gc events
    if 'concurrent-cleanup-end' in cause: concurrent_cleanup_handler(prefix, cause, timestamp, collector, file_handler)
    elif 'concurrent-mark-end' in cause: concurrent_mark_handler(prefix, cause, timestamp, collector, file_handler)
    elif 'concurrent-root-region-scan-end' in cause: concurrent_root_region_scan_handler(prefix, cause, timestamp, collector, file_handler)
    elif 'GC cleanup' in cause: cleanup_handler(prefix, cause, timestamp, collector, file_handler)
    elif 'Full GC' in cause:
        collector['count']['fullgc'] += 1
        fullgc_handler(prefix, cause, timestamp, collector, file_handler)
    # process complex gc events
    else:
        if 'initial-mark' in cause:
            collector['count']['initialmark'] += 1
            initial_mark_handler(prefix, cause, timestamp, collector, file_handler)
        elif isPause('young', cause):
            collector['count']['young'] += 1
            young_pause_handler(prefix, cause, timestamp, collector, file_handler)
        elif isPause('mixed', cause):
            collector['count']['mixed'] += 1
            mixed_pause_handler(prefix, cause, timestamp, collector, file_handler)
        elif 'remark' in cause:
            collector['count']['remark'] += 1
            remark_handler(prefix, cause, timestamp, collector, file_handler)
        elif cause[-1] == ']': return

def process_gc_log(collector):

    prefix = collector['prefix']
    # get latest gc log to process
    gc_log = get_latest_gc_log(collector['log_dir'], collector['log_name_pattern'])

    # update current_file and current_file_pos if this is the first time to
    # process the gc log
    if collector['current_file'] != gc_log:
        collector['current_file'] = gc_log
        with open(gc_log, 'rb') as file_handler:
            collector['current_file_pos'] = get_file_end(file_handler)
        return
    try:
        with open(gc_log, 'rb') as file_handler:

            pos = collector['current_file_pos']
            collector['current_file_pos'] = get_file_end(file_handler)
            file_handler.seek(pos)

            # Do not use foreach loop because inside function process_gc_record
            # will call file_handler.readline(). The reason is that some GC
            # event are multiline and need to be processed as a whole
            while True:
                line = file_handler.readline()
                if len(line) == 0:
                    break
                pattern_name, matcher = match_pattern(line)
                if pattern_name == GC_START_TIME_PATTERN:
                    year, month, day, hour, minute, second, timezone = [int(matcher.group(i)) for i in range(1, 8)]
                    cause = matcher.group(8)
                    timestamp = true_unix_timestamp(year, month, day, hour, minute, second, timezone)
                    process_gc_record(prefix, file_handler, timestamp, cause, collector)
                else:
                    unmatched_gc_log(line)

        current_timestamp_in_sec = int(time.time())

        if not collector['timestamp'] is None:
            for gen, value in collector['gensize'].items():
                print("%s.gc.g1.gensize %s %s gen=%s" % (prefix, current_timestamp_in_sec, value, gen))

        # publish gc event count metrics
        for event, value in collector['count'].items():
            print("%s.gc.g1.event.count %s %s event=%s" % (prefix, current_timestamp_in_sec, value, event))

    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        utils.err(''.join(
            traceback.format_exception(exc_type, exc_value, exc_traceback)))

    return 0

def main():

    interval = g1gc_conf.get_interval()
    config = g1gc_conf.get_gc_config()
    counters = {'young': 0, 'mixed': 0, 'initialmark': 0,
                'remark': 0, 'fullgc': 0}
    gensize = {'eden': 0, 'survivor': 0, 'heap': 0}
    collector = {'timestamp': None,
                 'data': {},
                 'count': counters,
                 'gensize': gensize,
                 'current_file': None,
                 'current_file_pos': None,
                 'prefix': config['prefix'],
                 'log_dir': config['log_dir'],
                 'log_name_pattern': config['log_name_pattern']}

    while True:
        process_gc_log(collector)
        sys.stdout.flush()
        time.sleep(interval)

if __name__ == '__main__':
    exit(main())
