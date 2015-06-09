#!/usr/bin/python

from abc import abstractmethod
import sys
import time
import threading
import traceback

from . import java
from . import utils

# Metrics produced before current-time - MAX_METRIC_AGE_IN_SECONDS are
# ignored (i.e., not emitted)
MAX_METRIC_AGE_IN_SECONDS = 600

class JmxMonitor(threading.Thread):
    """
    Base class for implementing JMX-based metric monitors through
    ``com.stumbleupon.monitoring.jmx``.
    Extending classes must:
        - implement ``process_metric``
        - provide the running ``USER`` class attribute
        - provide the ``PROCESS_NAME`` class attribute
    """
    USER = None
    PROCESS_NAME = None

    # Map certain JVM stats so they are unique and shorter
    SHORT_SERVICE_NAMES = {
        "GarbageCollector": "gc",
        "OperatingSystem": "os",
        "Threading": "threads",
    }

    # each group is allocated its own lock for coordinating event emission
    _REPORT_LOCKS = {}

    def __init__(self, pid, cmd, watched_mbeans, group=None):
        threading.Thread.__init__(self)

        if group and group not in JmxMonitor._REPORT_LOCKS:
            JmxMonitor._REPORT_LOCKS[group] = threading.Lock()
            self.group = group

        self.pid = pid
        self.cmd = cmd
        self.daemon = True
        self._prev_timestamp = 0
        self._is_shutdown = False
        self._jmx_process = java.init_jmx_process(str(pid), *watched_mbeans)

        utils.err("Enabling JMX monitoring: %s pid: %s" % (cmd, pid))

    def kill_monitor(self):
        """Kills the JMX monitor sub-process."""
        subprocess = self._jmx_process

        # Clean up after ourselves.
        rv = subprocess.poll()
        if rv is None:
            subprocess.terminate()
            rv = subprocess.poll()
            if rv is None:
                subprocess.kill()  # Bang bang!
                rv = subprocess.wait()
        subprocess.stdout.close()
        utils.err("warning: proc exited %d" % rv)
        return rv

    # Override
    def run(self):
        value, mbean = None

        while self._jmx_process.poll() is None:
            line = self._jmx_process.stdout.readline()
            if len(line) < 4:
                utils.err("invalid line (too short): %r" % line)
                continue

            try:
                timestamp, metric, value, mbean = line.split("\t", 3)
            except ValueError:
                # ignore non-metric lines produced by jmx.jar
                if "java.lang.String" not in line:
                    utils.err("Can't split line: %r" % line)
                    continue

            if metric in java.IGNORED_METRICS:
                continue

            try:
                timestamp = self.sanitize_timestamp(timestamp)
            except ValueError as e:
                utils.err("Invalid timestamp on line: %r -- %s" % (line, e))

            self._prev_timestamp = timestamp
            self.process_metric(timestamp, metric, value, mbean)

        utils.err("Stopping JMX monitoring: %s pid: %s" % (self.cmd, self.pid))

    def emit(self, metric, timestamp, value, tags):
        if self.group:
            JmxMonitor._REPORT_LOCKS[self.group].acquire()
        try:
            sys.stdout.write("flume.%s %d %s%s\n" % (metric, timestamp, value, tags))
            sys.stdout.flush()
        finally:
            if self.group:
                JmxMonitor._REPORT_LOCKS[self.group].release()

    @abstractmethod
    def process_metric(self, timestamp, metric, value, mbean):
        """
        Abstract function called for each JMX metric.
        :param timestamp: time (seconds since epoch) the metric was captured
        :param metric: the metric name
        :param value: the metric value at ``timestamp``
        :param mbean: mbean description in the form ``domain:key=value,...,foo=bar``
        """
        raise NotImplementedError

    def sanitize_timestamp(self, timestamp):
        """
        Verify that the given timestamp is more recent than the previously
        processed timestamp and is no older than MAX_METRIC_AGE_IN_SECONDS.
        :param timestamp: a candidate timestamp (seconds since epoch)
        :return: a valid int timestamp (seconds since epoch)
        """
        timestamp = int(timestamp)
        if timestamp < time.time() - MAX_METRIC_AGE_IN_SECONDS:
            raise ValueError("timestamp too old: %d" % timestamp)
        if timestamp < self._prev_timestamp:
            raise ValueError("timestamp out of order: prev=%d, new=%d"
                             % (self._prev_timestamp, timestamp))
        return timestamp

    @classmethod
    def start_monitors(cls):
        """
        Initialize the monitor:
            - drop privileges to the provided ``cls.USER``
            - setup signal handles to gracefully terminate the monitor threads
            - start monitoring against all ``cls.PROCESS_NAME`` processes
        """
        if not cls.USER:
            raise AttributeError("no USER attribute found")
        if not cls.PROCESS_NAME:
            raise AttributeError("no PROCESS_NAME attribute found")

        monitors = {}
        procs = {}

        utils.drop_privileges(user=cls.USER)

        def kill_monitors(join=False):
            for mon in monitors.values():
                mon.kill_monitor()
                if join:
                    mon.join()

        utils.setup_signal_handlers(kill_monitors, join=True)

        try:
            while True:
                for pid, monitor in monitors.items():
                    if not monitor.is_alive():
                        monitor.kill_monitor()
                        del monitors[pid]

                for pid, cmd in procs.iteritems():
                    if pid not in monitors:
                        monitors[pid] = cls(pid, cmd)
                        monitors[pid].start()

                # HACK: jmx monitor seems not to emit results if attached too early
                # so we wait one interval before we start monitoring
                procs = java.list_procs(cls.PROCESS_NAME)
                time.sleep(60)
        except Exception as e:
            utils.err('Caught exception: %s' % e)
            traceback.print_exc(file=sys.stderr)
        finally:
            kill_monitors()
            time.sleep(60)
            return 0  # Ask the tcollector to re-spawn us.

    @staticmethod
    def group_metrics(metric):
        """
        The JMX metrics have per-request-type metrics like so:
            metricNameNumOps
            metricNameMinTime
            metricNameMaxTime
            metricNameAvgTime
        Group related metrics together in the same metric name, use tags
        to separate the different request types, so we end up with:
            numOps  op=metricName
            avgTime op=metricName
        etc, which makes it easier to graph things with the TSD.
        :param metric: the metric name
        :return: a tuple containing the metric name and corresponding tag.
            If the metric is as described above, the augmented metric name
            is returned, otherwise we return the original metric
        """
        tags = ""
        if metric.endswith("NumOps"):
            tags = " op=" + metric[:-len("NumOps")]
            metric = "numOps"
        elif metric.endswith("MinTime"):
            tags = " op=" + metric[:-len("MinTime")]
            metric = "minTime"
        elif metric.endswith("AvgTime"):
            tags = " op=" + metric[:-len("AvgTime")]
            metric = "avgTime"
        elif metric.endswith("MaxTime"):
            tags = " op=" + metric[:-len("MaxTime")]
            metric = "maxTime"

        return metric, tags
