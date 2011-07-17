#!/usr/bin/env python
# Load average monitoring on FreeBSD for tcollector.
#
# By: Sean Rees <sean@rees.us>
# May, 2011
#
"""Import all numeric sysctl variables."""

import subprocess
import sys
import time

COLLECTION_INTERVAL = 15  # seconds

def labelify_cptimes(cp_times_list, cpu_label="all"):
    cptime_types = ["user", "nice", "system", "interrupt", "idle"]
    labeled_results = []

    for i in range(len(cptime_types)):
        labels = "cpu=%s type=%s" % (cpu_label, cptime_types[i])

        labeled_results.append((cp_times_list[i], labels))

    return labeled_results

def parse_kern_cptime(sysctls, key, kern_cptime_val, results):
    cpu_times = kern_cptime_val.split(" ")

    results[key] = labelify_cptimes(cpu_times)

def parse_kern_cptimes(sysctls, key, kern_cptimes_val, results):
    """Processes kern.cp_times.

    kern.cp_times is kern.cp_time for a single CPU. For multiple
    processors or cores, it repeats the values for kern.cp_time
    for each CPU with values for that CPU.

    We split the value along those lines and feed it into
    parse_kern_cptime.
    """
    # XXX: we presume there are always 5 cpu time types. This should
    # probably be less dumb.
    slice_len = 5

    num_cpus = int(sysctls['hw.ncpu'])
    cpu_times = kern_cptimes_val.split(" ")

    labeled_results = []

    for i in range(num_cpus):
        times_for_this_cpu = cpu_times[i*slice_len:i*slice_len+slice_len]
        labeled_results += labelify_cptimes(times_for_this_cpu, 'cpu%d' % i)

    results[key] = labeled_results

def parse_vm_loadavg(sysctls, key, loadavg_val, results):
    # vm.loadavg: { 1m 5m 15m }
    load_avgs = loadavg_val[2:-2].split(" ")

    results[key] = [
        (load_avgs[0], "interval=1m"),
        (load_avgs[1], "interval=5m"),
        (load_avgs[2], "interval=15m")
    ]

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

def main():
    """main loop"""

    # Some FreeBSD sysctl's are in a packed format, these extractors
    # will split out individual values.
    extractors = {
        "kern.cp_time": parse_kern_cptime,
        "kern.cp_times": parse_kern_cptimes,
        "vm.loadavg": parse_vm_loadavg,
        "vm.stats.vm.v_active_count": multiply_by_pagesize,
        "vm.stats.vm.v_inactive_count": multiply_by_pagesize,
        "vm.stats.vm.v_free_count": multiply_by_pagesize,
        "vm.stats.vm.v_cache_count": multiply_by_pagesize,
        "vm.stats.vm.v_wire_count": multiply_by_pagesize,
        "vm.stats.vm.v_page_count": multiply_by_pagesize
    }

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
    main()
