#!/usr/bin/env python
# General system monitoring via sysctl for FreeBSD.
#
# By: Sean Rees <sean@rees.us>
# July 2011.
#
"""Import all numeric sysctl variables."""

import subprocess
import sys
import time

# This has to be a long-running collector, because we need to
# sample kern.cp_times for deltas.
COLLECTION_INTERVAL = 15  # seconds

# We only sample kern.cp_times. On uni-processor systems
# it will equal kern.cp_time, on multi-processor systems,
# we'll get a (user, nice, system, interrupt, idle) block
# for each CPU.
CPTIMES_TYPES = ["user", "nice", "system", "interrupt", "idle"]
LAST_CPTIMES = None

def compute_and_label_times(cur_times, last_times, cpu_label="cpu0"):
    """Computes and labels results.

    This will diff each cpu time counter with the last value collected,
    then use that to form a percentage over the total difference since
    the last collection.

    Args:
        cur_times: list of current cpu times for a given cpu.
        last_times: the last sampled times for the same cpu.
        cpu_label: a human-friendly label for this cpu.

    Returns:
        A list of (cpu time percentage, {labels}) tuples.
    """
    global CPTIMES_TYPES
    labeled_results = []

    # Sanity checks, we should be comparing apples to apples here.
    assert len(cur_times) == len(last_times)
    assert len(cur_times) == len(CPTIMES_TYPES)

    # We compute percentages by diffing the ticks since the
    # last time, then determining the average of them all.
    differences = [(int)(cur_times[i]) - (int)(last_times[i])
                   for i in range(len(cur_times))]
    total_difference = sum(differences)

    for i in range(len(CPTIMES_TYPES)):
        labels = "cpu=%s type=%s" % (cpu_label, CPTIMES_TYPES[i])
        pct = round(float(differences[i]) / total_difference * 100, 1)

        labeled_results.append((pct, labels))

    return labeled_results

def parse_kern_cptimes(sysctls, key, kern_cptimes_val, results):
    """Processes kern.cp_times.

    kern.cp_times is kern.cp_time for a single CPU. For multiple
    processors or cores, it repeats the values for kern.cp_time
    for each CPU with values for that CPU.

    We split the value along those lines and feed it into
    parse_kern_cptime.
    """
    global CPTIMES_TYPES, LAST_CPTIMES

    slice_len = len(CPTIMES_TYPES)

    num_cpus = int(sysctls['hw.ncpu'])
    cpu_times = kern_cptimes_val.split(" ")

    labeled_results = []

    if LAST_CPTIMES is not None:
        for i in range(num_cpus):
            range_beg = i * slice_len
            range_end = range_beg + slice_len

            times_for_this_cpu = cpu_times[range_beg:range_end]
            last_times_for_this_cpu = LAST_CPTIMES[range_beg:range_end]

            labeled_results += compute_and_label_times(
                times_for_this_cpu, last_times_for_this_cpu, 'cpu%d' % i)

    LAST_CPTIMES = cpu_times

    # Compatible name with the Linux collector.
    results["proc.stat.cpu"] = labeled_results

def parse_vm_loadavg(sysctls, key, loadavg_val, results):
    # vm.loadavg: { 1m 5m 15m }
    load_avgs = loadavg_val[2:-2].split(" ")

    # Compatible names with the Linux collector.
    results["proc.loadavg.1min"] = load_avgs[0]
    results["proc.loadavg.5min"] = load_avgs[1]
    results["proc.loadavg.15min"] = load_avgs[2]

def multiply_by_pagesize(sysctls, key, val, results):
    page_size = int(sysctls["vm.stats.vm.v_page_size"])

    # We're changing from page count to a raw size, so rename
    # the variable.
    size_key = key.replace("_count", "_cmp_size")

    results[size_key] = int(val) * page_size
    results[key] = int(val)

def default_extractor(sysctls, key, value, results):
    try:
        value = int(value)
        results[key] = value
    except:
        pass

def sysctl_all():
    proc = subprocess.Popen(["/sbin/sysctl", "-a"], stdout=subprocess.PIPE)
    output = proc.communicate()[0]

    ret = {}
    if output:
        lines = output.split("\n")
        for line in lines:
            if ':' in line:
                name, value = line.split(":", 1)

                ret[name] = value.strip()
    return ret

def main(argv):
    """main loop"""

    # Some FreeBSD sysctl's are in a packed format, these extractors
    # will split out individual values.
    extractors = {
        "kern.cp_times": parse_kern_cptimes,
        "vm.loadavg": parse_vm_loadavg,
        "vm.stats.vm.v_active_count": multiply_by_pagesize,
        "vm.stats.vm.v_inactive_count": multiply_by_pagesize,
        "vm.stats.vm.v_free_count": multiply_by_pagesize,
        "vm.stats.vm.v_cache_count": multiply_by_pagesize,
        "vm.stats.vm.v_wire_count": multiply_by_pagesize,
        "vm.stats.vm.v_page_count": multiply_by_pagesize
    }

    # TODO: add some way to filter these results in
    # a sane way.
    while True:
        ts = int(time.time())

        sysctls = sysctl_all()
        results = {}

        for key, val in sysctls.iteritems():
            extractors.get(key, default_extractor)(
                sysctls, key, val, results)

        for key, val in results.iteritems():
            if isinstance(val, list):
                for labeled_val, labels in val:
                    print "%s %d %s %s" % (key, ts, labeled_val, labels)
            else:
                print "%s %d %s" % (key, ts, val)

        sys.stdout.flush()
        time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
    sys.exit(main(sys.argv))
