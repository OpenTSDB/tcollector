#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2010  The tcollector Authors.
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
# tcollector.py
#
"""Simple manager for collection scripts that run and gather data.
   The tcollector gathers the data and sends it to the TSD for storage."""
#
# by Mark Smith <msmith@stumbleupon.com>.
#

import atexit
import errno
import fcntl
import logging
import os
import random
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import json
import urllib2
import base64
from logging.handlers import RotatingFileHandler
from Queue import Queue
from Queue import Empty
from Queue import Full
from optparse import OptionParser


# global variables.
COLLECTORS = {}
GENERATION = 0
DEFAULT_LOG = '/var/log/tcollector.log'
LOG = logging.getLogger('tcollector')
ALIVE = True
# If the SenderThread catches more than this many consecutive uncaught
# exceptions, something is not right and tcollector will shutdown.
# Hopefully some kind of supervising daemon will then restart it.
MAX_UNCAUGHT_EXCEPTIONS = 100
DEFAULT_PORT = 4242
MAX_REASONABLE_TIMESTAMP = 1600000000  # Good until September 2020 :)
# How long to wait for datapoints before assuming
# a collector is dead and restarting it
ALLOWED_INACTIVITY_TIME = 600  # seconds
MAX_SENDQ_SIZE = 10000
MAX_READQ_SIZE = 100000


def register_collector(collector):
    """Register a collector with the COLLECTORS global"""

    assert isinstance(collector, Collector), "collector=%r" % (collector,)
    # store it in the global list and initiate a kill for anybody with the
    # same name that happens to still be hanging around
    if collector.name in COLLECTORS:
        col = COLLECTORS[collector.name]
        if col.proc is not None:
            LOG.error('%s still has a process (pid=%d) and is being reset,'
                      ' terminating', col.name, col.proc.pid)
            col.shutdown()

    COLLECTORS[collector.name] = collector


class ReaderQueue(Queue):
    """A Queue for the reader thread"""

    def nput(self, value):
        """A nonblocking put, that simply logs and discards the value when the
           queue is full, and returns false if we dropped."""
        try:
            self.put(value, False)
        except Full:
            LOG.error("DROPPED LINE: %s", value)
            return False
        return True


class Collector(object):
    """A Collector is a script that is run that gathers some data
       and prints it out in standard TSD format on STDOUT.  This
       class maintains all of the state information for a given
       collector and gives us utility methods for working with
       it."""

    def __init__(self, colname, interval, filename, mtime=0, lastspawn=0):
        """Construct a new Collector."""
        self.name = colname
        self.interval = interval
        self.filename = filename
        self.lastspawn = lastspawn
        self.proc = None
        self.nextkill = 0
        self.killstate = 0
        self.dead = False
        self.mtime = mtime
        self.generation = GENERATION
        self.buffer = ""
        self.datalines = []
        # Maps (metric, tags) to (value, repeated, line, timestamp) where:
        #  value: Last value seen.
        #  repeated: boolean, whether the last value was seen more than once.
        #  line: The last line that was read from that collector.
        #  timestamp: Time at which we saw the value for the first time.
        # This dict is used to keep track of and remove duplicate values.
        # Since it might grow unbounded (in case we see many different
        # combinations of metrics and tags) someone needs to regularly call
        # evict_old_keys() to remove old entries.
        self.values = {}
        self.lines_sent = 0
        self.lines_received = 0
        self.lines_invalid = 0
        self.last_datapoint = int(time.time())

    def read(self):
        """Read bytes from our subprocess and store them in our temporary
           line storage buffer.  This needs to be non-blocking."""

        # we have to use a buffer because sometimes the collectors
        # will write out a bunch of data points at one time and we
        # get some weird sized chunk.  This read call is non-blocking.

        # now read stderr for log messages, we could buffer here but since
        # we're just logging the messages, I don't care to
        try:
            out = self.proc.stderr.read()
            if out:
                LOG.debug('reading %s got %d bytes on stderr',
                          self.name, len(out))
                for line in out.splitlines():
                    LOG.warning('%s: %s', self.name, line)
        except IOError, (err, msg):
            if err != errno.EAGAIN:
                raise
        except:
            LOG.exception('uncaught exception in stderr read')

        # we have to use a buffer because sometimes the collectors will write
        # out a bunch of data points at one time and we get some weird sized
        # chunk.  This read call is non-blocking.
        try:
            self.buffer += self.proc.stdout.read()
            if len(self.buffer):
                LOG.debug('reading %s, buffer now %d bytes',
                          self.name, len(self.buffer))
        except IOError, (err, msg):
            if err != errno.EAGAIN:
                raise
        except AttributeError:
            # sometimes the process goes away in another thread and we don't
            # have it anymore, so log an error and bail
            LOG.exception('caught exception, collector process went away while reading stdout')
        except:
            LOG.exception('uncaught exception in stdout read')
            return

        # iterate for each line we have
        while self.buffer:
            idx = self.buffer.find('\n')
            if idx == -1:
                break

            # one full line is now found and we can pull it out of the buffer
            line = self.buffer[0:idx].strip()
            if line:
                self.datalines.append(line)
                self.last_datapoint = int(time.time())
            self.buffer = self.buffer[idx+1:]

    def collect(self):
        """Reads input from the collector and returns the lines up to whomever
           is calling us.  This is a generator that returns a line as it
           becomes available."""

        while self.proc is not None:
            self.read()
            if not len(self.datalines):
                return
            while len(self.datalines):
                yield self.datalines.pop(0)

    def shutdown(self):
        """Cleanly shut down the collector"""

        if not self.proc:
            return
        try:
            if self.proc.poll() is None:
                kill(self.proc)
                for attempt in range(5):
                    if self.proc.poll() is not None:
                        return
                    LOG.info('Waiting %ds for PID %d (%s) to exit...'
                             % (5 - attempt, self.proc.pid, self.name))
                    time.sleep(1)
                kill(self.proc, signal.SIGKILL)
                self.proc.wait()
        except:
            # we really don't want to die as we're trying to exit gracefully
            LOG.exception('ignoring uncaught exception while shutting down')

    def evict_old_keys(self, cut_off):
        """Remove old entries from the cache used to detect duplicate values.

        Args:
          cut_off: A UNIX timestamp.  Any value that's older than this will be
            removed from the cache.
        """
        for key in self.values.keys():
            time = self.values[key][3]
            if time < cut_off:
                del self.values[key]


class StdinCollector(Collector):
    """A StdinCollector simply reads from STDIN and provides the
       data.  This collector presents a uniform interface for the
       ReaderThread, although unlike a normal collector, read()/collect()
       will be blocking."""

    def __init__(self):
        super(StdinCollector, self).__init__('stdin', 0, '<stdin>')

        # hack to make this work.  nobody else will rely on self.proc
        # except as a test in the stdin mode.
        self.proc = True

    def read(self):
        """Read lines from STDIN and store them.  We allow this to
           be blocking because there should only ever be one
           StdinCollector and no normal collectors, so the ReaderThread
           is only serving us and we're allowed to block it."""

        global ALIVE
        line = sys.stdin.readline()
        if line:
            self.datalines.append(line.rstrip())
        else:
            ALIVE = False


    def shutdown(self):

        pass


class ReaderThread(threading.Thread):
    """The main ReaderThread is responsible for reading from the collectors
       and assuring that we always read from the input no matter what.
       All data read is put into the self.readerq Queue, which is
       consumed by the SenderThread."""

    def __init__(self, dedupinterval, evictinterval):
        """Constructor.
            Args:
              dedupinterval: If a metric sends the same value over successive
                intervals, suppress sending the same value to the TSD until
                this many seconds have elapsed.  This helps graphs over narrow
                time ranges still see timeseries with suppressed datapoints.
              evictinterval: In order to implement the behavior above, the
                code needs to keep track of the last value seen for each
                combination of (metric, tags).  Values older than
                evictinterval will be removed from the cache to save RAM.
                Invariant: evictinterval > dedupinterval
        """
        assert evictinterval > dedupinterval, "%r <= %r" % (evictinterval,
                                                            dedupinterval)
        super(ReaderThread, self).__init__()

        self.readerq = ReaderQueue(MAX_READQ_SIZE)
        self.lines_collected = 0
        self.lines_dropped = 0
        self.dedupinterval = dedupinterval
        self.evictinterval = evictinterval

    def run(self):
        """Main loop for this thread.  Just reads from collectors,
           does our input processing and de-duping, and puts the data
           into the queue."""

        LOG.debug("ReaderThread up and running")

        lastevict_time = 0
        # we loop every second for now.  ideally we'll setup some
        # select or other thing to wait for input on our children,
        # while breaking out every once in a while to setup selects
        # on new children.
        while ALIVE:
            alc = all_living_collectors()
            for col in alc:
                for line in col.collect():
                    self.process_line(col, line)

            if self.dedupinterval != 0:  # if 0 we do not use dedup
                now = int(time.time())
                if now - lastevict_time > self.evictinterval:
                    lastevict_time = now
                    now -= self.evictinterval
                    for col in all_collectors():
                        col.evict_old_keys(now)

            # and here is the loop that we really should get rid of, this
            # just prevents us from spinning right now
            time.sleep(1)

    def process_line(self, col, line):
        """Parses the given line and appends the result to the reader queue."""

        self.lines_collected += 1

        col.lines_received += 1
        if len(line) >= 1024:  # Limit in net.opentsdb.tsd.PipelineFactory
            LOG.warning('%s line too long: %s', col.name, line)
            col.lines_invalid += 1
            return
        parsed = re.match('^([-_./a-zA-Z0-9]+)\s+' # Metric name.
                          '(\d+\.?\d+)\s+'               # Timestamp.
                          '(\S+?)'                 # Value (int or float).
                          '((?:\s+[-_./a-zA-Z0-9]+=[-_./a-zA-Z0-9]+)*)$', # Tags
                          line)
        if parsed is None:
            LOG.warning('%s sent invalid data: %s', col.name, line)
            col.lines_invalid += 1
            return
        metric, timestamp, value, tags = parsed.groups()
        timestamp = int(timestamp)

        # If there are more than 11 digits we're dealing with a timestamp
        # with millisecond precision
        if len(str(timestamp)) > 11:
            global MAX_REASONABLE_TIMESTAMP
            MAX_REASONABLE_TIMESTAMP = MAX_REASONABLE_TIMESTAMP * 1000

        # De-dupe detection...  To reduce the number of points we send to the
        # TSD, we suppress sending values of metrics that don't change to
        # only once every 10 minutes (which is also when TSD changes rows
        # and how much extra time the scanner adds to the beginning/end of a
        # graph interval in order to correctly calculate aggregated values).
        # When the values do change, we want to first send the previous value
        # with what the timestamp was when it first became that value (to keep
        # slopes of graphs correct).
        #
        if self.dedupinterval != 0:  # if 0 we do not use dedup
            key = (metric, tags)
            if key in col.values:
                # if the timestamp isn't > than the previous one, ignore this value
                if timestamp <= col.values[key][3]:
                    LOG.error("Timestamp out of order: metric=%s%s,"
                              " old_ts=%d >= new_ts=%d - ignoring data point"
                              " (value=%r, collector=%s)", metric, tags,
                              col.values[key][3], timestamp, value, col.name)
                    col.lines_invalid += 1
                    return
                elif timestamp >= MAX_REASONABLE_TIMESTAMP:
                    LOG.error("Timestamp is too far out in the future: metric=%s%s"
                              " old_ts=%d, new_ts=%d - ignoring data point"
                              " (value=%r, collector=%s)", metric, tags,
                              col.values[key][3], timestamp, value, col.name)
                    return

                # if this data point is repeated, store it but don't send.
                # store the previous timestamp, so when/if this value changes
                # we send the timestamp when this metric first became the current
                # value instead of the last.  Fall through if we reach
                # the dedup interval so we can print the value.
                if (col.values[key][0] == value and
                    (timestamp - col.values[key][3] < self.dedupinterval)):
                    col.values[key] = (value, True, line, col.values[key][3])
                    return

                # we might have to append two lines if the value has been the same
                # for a while and we've skipped one or more values.  we need to
                # replay the last value we skipped (if changed) so the jumps in
                # our graph are accurate,
                if ((col.values[key][1] or
                    (timestamp - col.values[key][3] >= self.dedupinterval))
                    and col.values[key][0] != value):
                    col.lines_sent += 1
                    if not self.readerq.nput(col.values[key][2]):
                        self.lines_dropped += 1

            # now we can reset for the next pass and send the line we actually
            # want to send
            # col.values is a dict of tuples, with the key being the metric and
            # tags (essentially the same as wthat TSD uses for the row key).
            # The array consists of:
            # [ the metric's value, if this value was repeated, the line of data,
            #   the value's timestamp that it last changed ]
            col.values[key] = (value, False, line, timestamp)

        col.lines_sent += 1
        if not self.readerq.nput(line):
            self.lines_dropped += 1


class SenderThread(threading.Thread):
    """The SenderThread is responsible for maintaining a connection
       to the TSD and sending the data we're getting over to it.  This
       thread is also responsible for doing any sort of emergency
       buffering we might need to do if we can't establish a connection
       and we need to spool to disk.  That isn't implemented yet."""

    def __init__(self, reader, dryrun, hosts, self_report_stats, tags,
                 reconnectinterval=0, http=False, http_username=None,
                 http_password=None, ssl=False, maxtags=8):
        """Constructor.

        Args:
          reader: A reference to a ReaderThread instance.
          dryrun: If true, data points will be printed on stdout instead of
            being sent to the TSD.
          hosts: List of (host, port) tuples defining list of TSDs
          self_report_stats: If true, the reader thread will insert its own
            stats into the metrics reported to TSD, as if those metrics had
            been read from a collector.
          http: A boolean that controls whether or not the http endpoint is used.
          ssl: A boolean that controls whether or not the http endpoint uses ssl.
          tags: A dictionary of tags to append for every data point.
        """
        super(SenderThread, self).__init__()

        self.dryrun = dryrun
        self.reader = reader
        self.tags = sorted(tags.items()) # dictionary transformed to list
        self.http = http
        self.http_username = http_username
        self.http_password = http_password
        self.ssl = ssl
        self.hosts = hosts  # A list of (host, port) pairs.
        # Randomize hosts to help even out the load.
        random.shuffle(self.hosts)
        self.blacklisted_hosts = set()  # The 'bad' (host, port) pairs.
        self.current_tsd = -1  # Index in self.hosts where we're at.
        self.host = None  # The current TSD host we've selected.
        self.port = None  # The port of the current TSD.
        self.tsd = None   # The socket connected to the aforementioned TSD.
        self.last_verify = 0
        self.reconnectinterval = reconnectinterval # in seconds.
        self.time_reconnect = 0  # if reconnectinterval > 0, used to track the time.
        self.sendq = []
        self.self_report_stats = self_report_stats
        self.maxtags = maxtags # The maximum number of tags TSD will accept.

    def pick_connection(self):
        """Picks up a random host/port connection."""
        # Try to get the next host from the list, until we find a host that
        # isn't in the blacklist, or until we run out of hosts (i.e. they
        # are all blacklisted, which typically happens when we lost our
        # connectivity to the outside world).
        for self.current_tsd in xrange(self.current_tsd + 1, len(self.hosts)):
            hostport = self.hosts[self.current_tsd]
            if hostport not in self.blacklisted_hosts:
                break
        else:
            LOG.info('No more healthy hosts, retry with previously blacklisted')
            random.shuffle(self.hosts)
            self.blacklisted_hosts.clear()
            self.current_tsd = 0
            hostport = self.hosts[self.current_tsd]
        self.host, self.port = hostport
        LOG.info('Selected connection: %s:%d', self.host, self.port)

    def blacklist_connection(self):
        """Marks the current TSD host we're trying to use as blacklisted.

           Blacklisted hosts will get another chance to be elected once there
           will be no more healthy hosts."""
        # FIXME: Enhance this naive strategy.
        LOG.info('Blacklisting %s:%s for a while', self.host, self.port)
        self.blacklisted_hosts.add((self.host, self.port))

    def run(self):
        """Main loop.  A simple scheduler.  Loop waiting for 5
           seconds for data on the queue.  If there's no data, just
           loop and make sure our connection is still open.  If there
           is data, wait 5 more seconds and grab all of the pending data and
           send it.  A little better than sending every line as its
           own packet."""

        errors = 0  # How many uncaught exceptions in a row we got.
        while ALIVE:
            try:
                self.maintain_conn()
                try:
                    line = self.reader.readerq.get(True, 5)
                except Empty:
                    continue
                self.sendq.append(line)
                time.sleep(5)  # Wait for more data
                while True:
                    # prevents self.sendq fast growing in case of sending fails
                    # in send_data()
                    if len(self.sendq) > MAX_SENDQ_SIZE:
                        break
                    try:
                        line = self.reader.readerq.get(False)
                    except Empty:
                        break
                    self.sendq.append(line)

                if ALIVE:
                    self.send_data()
                errors = 0  # We managed to do a successful iteration.
            except (ArithmeticError, EOFError, EnvironmentError, LookupError,
                    ValueError), e:
                errors += 1
                if errors > MAX_UNCAUGHT_EXCEPTIONS:
                    shutdown()
                    raise
                LOG.exception('Uncaught exception in SenderThread, ignoring')
                time.sleep(1)
                continue
            except:
                LOG.exception('Uncaught exception in SenderThread, going to exit')
                shutdown()
                raise

    def verify_conn(self):
        """Periodically verify that our connection to the TSD is OK
           and that the TSD is alive/working."""
        # http connections don't need this
        if self.http:
            return True

        if self.tsd is None:
            return False

        # if the last verification was less than a minute ago, don't re-verify
        if self.last_verify > time.time() - 60:
            return True

        # in case reconnect is activated, check if it's time to reconnect
        if self.reconnectinterval > 0 and self.time_reconnect < time.time() - self.reconnectinterval:
            # closing the connection and indicating that we need to reconnect.
            try:
                self.tsd.close()
            except socket.error, msg:
                pass    # not handling that
            self.time_reconnect = time.time()
            return False

        # we use the version command as it is very low effort for the TSD
        # to respond
        LOG.debug('verifying our TSD connection is alive')
        try:
            self.tsd.sendall('version\n')
        except socket.error, msg:
            self.tsd = None
            self.blacklist_connection()
            return False

        bufsize = 4096
        while ALIVE:
            # try to read as much data as we can.  at some point this is going
            # to block, but we have set the timeout low when we made the
            # connection
            try:
                buf = self.tsd.recv(bufsize)
            except socket.error, msg:
                self.tsd = None
                self.blacklist_connection()
                return False

            # If we don't get a response to the `version' request, the TSD
            # must be dead or overloaded.
            if not buf:
                self.tsd = None
                self.blacklist_connection()
                return False

            # Woah, the TSD has a lot of things to tell us...  Let's make
            # sure we read everything it sent us by looping once more.
            if len(buf) == bufsize:
                continue

            # If everything is good, send out our meta stats.  This
            # helps to see what is going on with the tcollector.
            # TODO need to fix this for http
            if self.self_report_stats:
                strs = [
                        ('reader.lines_collected',
                         '', self.reader.lines_collected),
                        ('reader.lines_dropped',
                         '', self.reader.lines_dropped)
                       ]

                for col in all_living_collectors():
                    strs.append(('collector.lines_sent', 'collector='
                                 + col.name, col.lines_sent))
                    strs.append(('collector.lines_received', 'collector='
                                 + col.name, col.lines_received))
                    strs.append(('collector.lines_invalid', 'collector='
                                 + col.name, col.lines_invalid))

                ts = int(time.time())
                strout = ["tcollector.%s %d %d %s"
                          % (x[0], ts, x[2], x[1]) for x in strs]
                for string in strout:
                    self.sendq.append(string)

            break  # TSD is alive.

        # if we get here, we assume the connection is good
        self.last_verify = time.time()
        return True

    def maintain_conn(self):
        """Safely connect to the TSD and ensure that it's up and
           running and that we're not talking to a ghost connection
           (no response)."""

        # dry runs and http are always good
        if self.dryrun or self.http:
            return

        # connection didn't verify, so create a new one.  we might be in
        # this method for a long time while we sort this out.
        try_delay = 1
        while ALIVE:
            if self.verify_conn():
                return

            # increase the try delay by some amount and some random value,
            # in case the TSD is down for a while.  delay at most
            # approximately 10 minutes.
            try_delay *= 1 + random.random()
            if try_delay > 600:
                try_delay *= 0.5
            LOG.debug('SenderThread blocking %0.2f seconds', try_delay)
            time.sleep(try_delay)

            # Now actually try the connection.
            self.pick_connection()
            try:
                addresses = socket.getaddrinfo(self.host, self.port,
                                               socket.AF_UNSPEC,
                                               socket.SOCK_STREAM, 0)
            except socket.gaierror, e:
                # Don't croak on transient DNS resolution issues.
                if e[0] in (socket.EAI_AGAIN, socket.EAI_NONAME,
                            socket.EAI_NODATA):
                    LOG.debug('DNS resolution failure: %s: %s', self.host, e)
                    continue
                raise
            for family, socktype, proto, canonname, sockaddr in addresses:
                try:
                    self.tsd = socket.socket(family, socktype, proto)
                    self.tsd.settimeout(15)
                    self.tsd.connect(sockaddr)
                    # if we get here it connected
                    LOG.debug('Connection to %s was successful'%(str(sockaddr)))
                    break
                except socket.error, msg:
                    LOG.warning('Connection attempt failed to %s:%d: %s',
                                self.host, self.port, msg)
                self.tsd.close()
                self.tsd = None
            if not self.tsd:
                LOG.error('Failed to connect to %s:%d', self.host, self.port)
                self.blacklist_connection()

    def add_tags_to_line(self, line):
        for tag, value in self.tags:
            if ' %s=' % tag not in line:
                line += ' %s=%s' % (tag, value)
        return line

    def send_data(self):
        """Sends outstanding data in self.sendq to the TSD in one operation."""
        if self.http:
            return self.send_data_via_http()

        # construct the output string
        out = ''

        # in case of logging we use less efficient variant
        if LOG.level == logging.DEBUG:
            for line in self.sendq:
                line = "put %s" % self.add_tags_to_line(line)
                out += line + "\n"
                LOG.debug('SENDING: %s', line)
        else:
            out = "".join("put %s\n" % self.add_tags_to_line(line) for line in self.sendq)

        if not out:
            LOG.debug('send_data no data?')
            return

        # try sending our data.  if an exception occurs, just error and
        # try sending again next time.
        try:
            if self.dryrun:
                print out
            else:
                self.tsd.sendall(out)
            self.sendq = []
        except socket.error, msg:
            LOG.error('failed to send data: %s', msg)
            try:
                self.tsd.close()
            except socket.error:
                pass
            self.tsd = None
            self.blacklist_connection()

        # FIXME: we should be reading the result at some point to drain
        # the packets out of the kernel's queue

    def send_data_via_http(self):
        """Sends outstanding data in self.sendq to TSD in one HTTP API call."""
        metrics = []
        for line in self.sendq:
            # print " %s" % line
            parts = line.split(None, 3)
            # not all metrics have metric-specific tags
            if len(parts) == 4:
              (metric, timestamp, value, raw_tags) = parts
            else:
              (metric, timestamp, value) = parts
              raw_tags = ""
            # process the tags
            metric_tags = {}
            for tag in raw_tags.strip().split():
                (tag_key, tag_value) = tag.split("=", 1)
                metric_tags[tag_key] = tag_value
            metric_entry = {}
            metric_entry["metric"] = metric
            metric_entry["timestamp"] = long(timestamp)
            metric_entry["value"] = float(value)
            metric_entry["tags"] = dict(self.tags).copy()
            if len(metric_tags) + len(metric_entry["tags"]) > self.maxtags:
              metric_tags_orig = set(metric_tags)
              subset_metric_keys = frozenset(metric_tags[:len(metric_tags[:self.maxtags-len(metric_entry["tags"])])])
              metric_tags = dict((k, v) for k, v in metric_tags.iteritems() if k in subset_metric_keys)
              LOG.error("Exceeding maximum permitted metric tags - removing %s for metric %s",
                        str(metric_tags_orig - set(metric_tags)), metric)
            metric_entry["tags"].update(metric_tags)
            metrics.append(metric_entry)

        if self.dryrun:
            print "Would have sent:\n%s" % json.dumps(metrics,
                                                      sort_keys=True,
                                                      indent=4)
            return

        if((self.current_tsd == -1) or (len(self.hosts) > 1)):
            self.pick_connection()
        # print "Using server: %s:%s" % (self.host, self.port)
        # url = "http://%s:%s/api/put?details" % (self.host, self.port)
        # print "Url is %s" % url
        if self.ssl:
            protocol = "https"
        else:
            protocol = "http"
        LOG.debug("Sending metrics to %s://%s:%s/api/put?details",
                  protocol, self.host, self.port)
        details=""
        if LOG.level == logging.DEBUG:
            details="?details"
        req = urllib2.Request("%s://%s:%s/api/put%s" % (
            protocol, self.host, self.port, details))
        if self.http_username and self.http_password:
          req.add_header("Authorization", "Basic %s"
                         % base64.b64encode("%s:%s" % (self.http_username, self.http_password)))
        req.add_header("Content-Type", "application/json")
        try:
            response = urllib2.urlopen(req, json.dumps(metrics))
            LOG.debug("Received response %s %s", response.getcode(), response.read().rstrip('\n'))
            # clear out the sendq
            self.sendq = []
            # print "Got response code: %s" % response.getcode()
            # print "Content:"
            # for line in response:
            #     print line,
            #     print
        except urllib2.HTTPError, e:
            LOG.error("Got error %s %s", e, e.read().rstrip('\n'))
            # for line in http_error:
            #   print line,


def setup_logging(logfile=DEFAULT_LOG, max_bytes=None, backup_count=None):
    """Sets up logging and associated handlers."""

    LOG.setLevel(logging.INFO)
    if backup_count is not None and max_bytes is not None:
        assert backup_count > 0
        assert max_bytes > 0
        ch = RotatingFileHandler(logfile, 'a', max_bytes, backup_count)
    else:  # Setup stream handler.
        ch = logging.StreamHandler(sys.stdout)

    ch.setFormatter(logging.Formatter('%(asctime)s %(name)s[%(process)d] '
                                      '%(levelname)s: %(message)s'))
    LOG.addHandler(ch)


def parse_cmdline(argv):
    """Parses the command-line."""

    try:
        from collectors.etc import config
        defaults = config.get_defaults()
    except ImportError:
        sys.stderr.write("ImportError: Could not load defaults from configuration. Using hardcoded values")
        default_cdir = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), 'collectors')
        defaults = {
            'verbose': False,
            'no_tcollector_stats': False,
            'evictinterval': 6000,
            'dedupinterval': 300,
            'allowed_inactivity_time': 600,
            'dryrun': False,
            'maxtags': 8,
            'max_bytes': 64 * 1024 * 1024,
            'http_password': False,
            'reconnectinterval': 0,
            'http_username': False,
            'port': 4242,
            'pidfile': '/var/run/tcollector.pid',
            'http': False,
            'tags': [],
            'remove_inactive_collectors': False,
            'host': 'localhost',
            'backup_count': 1,
            'logfile': '/var/log/tcollector.log',
            'cdir': default_cdir,
            'ssl': False,
            'stdin': False,
            'daemonize': False,
            'hosts': False
        }
    except:
        sys.stderr.write("Unexpected error: %s" % sys.exc_info()[0])
        raise

    # get arguments
    parser = OptionParser(description='Manages collectors which gather '
                                       'data and report back.')
    parser.add_option('-c', '--collector-dir', dest='cdir', metavar='DIR',
                        default=defaults['cdir'],
                        help='Directory where the collectors are located.')
    parser.add_option('-d', '--dry-run', dest='dryrun', action='store_true',
                        default=defaults['dryrun'],
                        help='Don\'t actually send anything to the TSD, '
                           'just print the datapoints.')
    parser.add_option('-D', '--daemonize', dest='daemonize', action='store_true',
                        default=defaults['daemonize'],
                        help='Run as a background daemon.')
    parser.add_option('-H', '--host', dest='host',
                        metavar='HOST',
                        default=defaults['host'],
                        help='Hostname to use to connect to the TSD.')
    parser.add_option('-L', '--hosts-list', dest='hosts',
                        metavar='HOSTS',
                        default=defaults['hosts'],
                        help='List of host:port to connect to tsd\'s (comma separated).')
    parser.add_option('--no-tcollector-stats', dest='no_tcollector_stats',
                        action='store_true',
                        default=defaults['no_tcollector_stats'],
                        help='Prevent tcollector from reporting its own stats to TSD')
    parser.add_option('-s', '--stdin', dest='stdin', action='store_true',
                        default=defaults['stdin'],
                        help='Run once, read and dedup data points from stdin.')
    parser.add_option('-p', '--port', dest='port', type='int',
                        default=defaults['port'], metavar='PORT',
                        help='Port to connect to the TSD instance on. '
                        'default=%default')
    parser.add_option('-v', dest='verbose', action='store_true',
                        default=defaults['verbose'],
                        help='Verbose mode (log debug messages).')
    parser.add_option('-t', '--tag', dest='tags', action='append',
                        default=defaults['tags'], metavar='TAG',
                        help='Tags to append to all timeseries we send, '
                            'e.g.: -t TAG=VALUE -t TAG2=VALUE')
    parser.add_option('-P', '--pidfile', dest='pidfile',
                        default=defaults['pidfile'],
                        metavar='FILE', help='Write our pidfile')
    parser.add_option('--dedup-interval', dest='dedupinterval', type='int',
                        default=defaults['dedupinterval'], metavar='DEDUPINTERVAL',
                        help='Number of seconds in which successive duplicate '
                           'datapoints are suppressed before sending to the TSD. '
                           'Use zero to disable. '
                           'default=%default')
    parser.add_option('--evict-interval', dest='evictinterval', type='int',
                        default=defaults['evictinterval'], metavar='EVICTINTERVAL',
                        help='Number of seconds after which to remove cached '
                           'values of old data points to save memory. '
                           'default=%default')
    parser.add_option('--allowed-inactivity-time', dest='allowed_inactivity_time', type='int',
                        default=ALLOWED_INACTIVITY_TIME, metavar='ALLOWEDINACTIVITYTIME',
                            help='How long to wait for datapoints before assuming '
                                'a collector is dead and restart it. '
                                'default=%default')
    parser.add_option('--remove-inactive-collectors', dest='remove_inactive_collectors', action='store_true',
                        default=defaults['remove_inactive_collectors'], help='Remove collectors not sending data '
                                          'in the max allowed inactivity interval')
    parser.add_option('--max-bytes', dest='max_bytes', type='int',
                        default=defaults['max_bytes'],
                        help='Maximum bytes per a logfile.')
    parser.add_option('--backup-count', dest='backup_count', type='int',
                        default=defaults['backup_count'], help='Maximum number of logfiles to backup.')
    parser.add_option('--logfile', dest='logfile', type='str',
                        default=DEFAULT_LOG,
                        help='Filename where logs are written to.')
    parser.add_option('--reconnect-interval',dest='reconnectinterval', type='int',
                        default=defaults['reconnectinterval'], metavar='RECONNECTINTERVAL',
                        help='Number of seconds after which the connection to'
                           'the TSD hostname reconnects itself. This is useful'
                           'when the hostname is a multiple A record (RRDNS).')
    parser.add_option('--max-tags', dest='maxtags', type=int, default=defaults['maxtags'],
                        help='The maximum number of tags to send to our TSD Instances')
    parser.add_option('--http', dest='http', action='store_true', default=defaults['http'],
                        help='Send the data via the http interface')
    parser.add_option('--http-username', dest='http_username', default=defaults['http_username'],
                      help='Username to use for HTTP Basic Auth when sending the data via HTTP')
    parser.add_option('--http-password', dest='http_password', default=defaults['http_password'],
                      help='Password to use for HTTP Basic Auth when sending the data via HTTP')
    parser.add_option('--ssl', dest='ssl', action='store_true', default=defaults['ssl'],
                      help='Enable SSL - used in conjunction with http')
    (options, args) = parser.parse_args(args=argv[1:])
    if options.dedupinterval < 0:
        parser.error('--dedup-interval must be at least 0 seconds')
    if options.evictinterval <= options.dedupinterval:
        parser.error('--evict-interval must be strictly greater than '
                     '--dedup-interval')
    if options.reconnectinterval < 0:
        parser.error('--reconnect-interval must be at least 0 seconds')
    # We cannot write to stdout when we're a daemon.
    if (options.daemonize or options.max_bytes) and not options.backup_count:
        options.backup_count = 1
    return (options, args)


def daemonize():
    """Performs the necessary dance to become a background daemon."""
    if os.fork():
        os._exit(0)
    os.chdir("/")
    os.umask(022)
    os.setsid()
    os.umask(0)
    if os.fork():
        os._exit(0)
    stdin = open(os.devnull)
    stdout = open(os.devnull, 'w')
    os.dup2(stdin.fileno(), 0)
    os.dup2(stdout.fileno(), 1)
    os.dup2(stdout.fileno(), 2)
    stdin.close()
    stdout.close()
    os.umask(022)
    for fd in xrange(3, 1024):
        try:
            os.close(fd)
        except OSError:  # This FD wasn't opened...
            pass         # ... ignore the exception.


def setup_python_path(collector_dir):
    """Sets up PYTHONPATH so that collectors can easily import common code."""
    mydir = os.path.dirname(collector_dir)
    libdir = os.path.join(mydir, 'collectors', 'lib')
    if not os.path.isdir(libdir):
        return
    pythonpath = os.environ.get('PYTHONPATH', '')
    if pythonpath:
        pythonpath += ':'
    pythonpath += mydir
    os.environ['PYTHONPATH'] = pythonpath
    LOG.debug('Set PYTHONPATH to %r', pythonpath)


def main(argv):
    """The main tcollector entry point and loop."""

    try:
        options, args = parse_cmdline(argv)
    except:
        sys.stderr.write("Unexpected error: %s" % sys.exc_info()[0])
        return 1

    if options.daemonize:
        daemonize()
    setup_logging(options.logfile, options.max_bytes or None,
                  options.backup_count or None)

    if options.verbose:
        LOG.setLevel(logging.DEBUG)  # up our level

    if options.pidfile:
        write_pid(options.pidfile)

    # validate everything
    tags = {}
    for tag in options.tags:
        if re.match('^[-_.a-z0-9]+=\S+$', tag, re.IGNORECASE) is None:
            assert False, 'Tag string "%s" is invalid.' % tag
        k, v = tag.split('=', 1)
        if k in tags:
            assert False, 'Tag "%s" already declared.' % k
        tags[k] = v

    if not 'host' in tags and not options.stdin:
        tags['host'] = socket.gethostname()
        LOG.warning('Tag "host" not specified, defaulting to %s.', tags['host'])

    options.cdir = os.path.realpath(options.cdir)
    if not os.path.isdir(options.cdir):
        LOG.fatal('No such directory: %s', options.cdir)
        return 1
    modules = load_etc_dir(options, tags)

    setup_python_path(options.cdir)

    # gracefully handle death for normal termination paths and abnormal
    atexit.register(shutdown)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, shutdown_signal)

    # at this point we're ready to start processing, so start the ReaderThread
    # so we can have it running and pulling in data for us
    reader = ReaderThread(options.dedupinterval, options.evictinterval)
    reader.start()

    # prepare list of (host, port) of TSDs given on CLI
    if not options.hosts:
        options.hosts = [(options.host, options.port)]
    else:
        def splitHost(hostport):
            if ":" in hostport:
                # Check if we have an IPv6 address.
                if hostport[0] == "[" and "]:" in hostport:
                    host, port = hostport.split("]:")
                    host = host[1:]
                else:
                    host, port = hostport.split(":")
                return (host, int(port))
            return (hostport, DEFAULT_PORT)
        options.hosts = [splitHost(host) for host in options.hosts.split(",")]
        if options.host != "localhost" or options.port != DEFAULT_PORT:
            options.hosts.append((options.host, options.port))

    # and setup the sender to start writing out to the tsd
    sender = SenderThread(reader, options.dryrun, options.hosts,
                          not options.no_tcollector_stats, tags, options.reconnectinterval,
                          options.http, options.http_username,
                          options.http_password, options.ssl, options.maxtags)
    sender.start()
    LOG.info('SenderThread startup complete')

    # if we're in stdin mode, build a stdin collector and just join on the
    # reader thread since there's nothing else for us to do here
    if options.stdin:
        register_collector(StdinCollector())
        stdin_loop(options, modules, sender, tags)
    else:
        sys.stdin.close()
        main_loop(options, modules, sender, tags)

    # We're exiting, make sure we don't leave any collector behind.
    for col in all_living_collectors():
      col.shutdown()
    LOG.debug('Shutting down -- joining the reader thread.')
    reader.join()
    LOG.debug('Shutting down -- joining the sender thread.')
    sender.join()

def stdin_loop(options, modules, sender, tags):
    """The main loop of the program that runs when we are in stdin mode."""

    global ALIVE
    next_heartbeat = int(time.time() + 600)
    while ALIVE:
        time.sleep(15)
        reload_changed_config_modules(modules, options, sender, tags)
        now = int(time.time())
        if now >= next_heartbeat:
            LOG.info('Heartbeat (%d collectors running)'
                     % sum(1 for col in all_living_collectors()))
            next_heartbeat = now + 600

def main_loop(options, modules, sender, tags):
    """The main loop of the program that runs when we're not in stdin mode."""

    next_heartbeat = int(time.time() + 600)
    while ALIVE:
        populate_collectors(options.cdir)
        reload_changed_config_modules(modules, options, sender, tags)
        reap_children()
        check_children(options)
        spawn_children()
        time.sleep(15)
        now = int(time.time())
        if now >= next_heartbeat:
            LOG.info('Heartbeat (%d collectors running)'
                     % sum(1 for col in all_living_collectors()))
            next_heartbeat = now + 600


def list_config_modules(etcdir):
    """Returns an iterator that yields the name of all the config modules."""
    if not os.path.isdir(etcdir):
        return iter(())  # Empty iterator.
    return (name for name in os.listdir(etcdir)
            if (name.endswith('.py')
                and os.path.isfile(os.path.join(etcdir, name))))


def load_etc_dir(options, tags):
    """Loads any Python module from tcollector's own 'etc' directory.

    Returns: A dict of path -> (module, timestamp).
    """

    etcdir = os.path.join(options.cdir, 'etc')
    sys.path.append(etcdir)  # So we can import modules from the etc dir.
    modules = {}  # path -> (module, timestamp)
    for name in list_config_modules(etcdir):
        path = os.path.join(etcdir, name)
        module = load_config_module(name, options, tags)
        modules[path] = (module, os.path.getmtime(path))
    return modules


def load_config_module(name, options, tags):
    """Imports the config module of the given name

    The 'name' argument can be a string, in which case the module will be
    loaded by name, or it can be a module object, in which case the module
    will get reloaded.

    If the module has an 'onload' function, calls it.
    Returns: the reference to the module loaded.
    """

    if isinstance(name, str):
      LOG.info('Loading %s', name)
      d = {}
      # Strip the trailing .py
      module = __import__(name[:-3], d, d)
    else:
      module = reload(name)
    onload = module.__dict__.get('onload')
    if callable(onload):
        try:
            onload(options, tags)
        except:
            LOG.fatal('Exception while loading %s', name)
            raise
    return module


def reload_changed_config_modules(modules, options, sender, tags):
    """Reloads any changed modules from the 'etc' directory.

    Args:
      cdir: The path to the 'collectors' directory.
      modules: A dict of path -> (module, timestamp).
    Returns: whether or not anything has changed.
    """

    etcdir = os.path.join(options.cdir, 'etc')
    current_modules = set(list_config_modules(etcdir))
    current_paths = set(os.path.join(etcdir, name)
                        for name in current_modules)
    changed = False

    # Reload any module that has changed.
    for path, (module, timestamp) in modules.iteritems():
        if path not in current_paths:  # Module was removed.
            continue
        mtime = os.path.getmtime(path)
        if mtime > timestamp:
            LOG.info('Reloading %s, file has changed', path)
            module = load_config_module(module, options, tags)
            modules[path] = (module, mtime)
            changed = True

    # Remove any module that has been removed.
    for path in set(modules).difference(current_paths):
        LOG.info('%s has been removed, tcollector should be restarted', path)
        del modules[path]
        changed = True

    # Check for any modules that may have been added.
    for name in current_modules:
        path = os.path.join(etcdir, name)
        if path not in modules:
            module = load_config_module(name, options, tags)
            modules[path] = (module, os.path.getmtime(path))
            changed = True

    return changed


def write_pid(pidfile):
    """Write our pid to a pidfile."""
    f = open(pidfile, "w")
    try:
        f.write(str(os.getpid()))
    finally:
        f.close()


def all_collectors():
    """Generator to return all collectors."""

    return COLLECTORS.itervalues()


# collectors that are not marked dead
def all_valid_collectors():
    """Generator to return all defined collectors that haven't been marked
       dead in the past hour, allowing temporarily broken collectors a
       chance at redemption."""

    now = int(time.time())
    for col in all_collectors():
        if not col.dead or (now - col.lastspawn > 3600):
            yield col


# collectors that have a process attached (currenty alive)
def all_living_collectors():
    """Generator to return all defined collectors that have
       an active process."""

    for col in all_collectors():
        if col.proc is not None:
            yield col


def shutdown_signal(signum, frame):
    """Called when we get a signal and need to terminate."""
    LOG.warning("shutting down, got signal %d", signum)
    shutdown()


def kill(proc, signum=signal.SIGTERM):
  os.killpg(proc.pid, signum)


def shutdown():
    """Called by atexit and when we receive a signal, this ensures we properly
       terminate any outstanding children."""

    global ALIVE
    # prevent repeated calls
    if not ALIVE:
        return
    # notify threads of program termination
    ALIVE = False

    LOG.info('shutting down children')

    # tell everyone to die
    for col in all_living_collectors():
        col.shutdown()

    LOG.info('exiting')
    sys.exit(1)


def reap_children():
    """When a child process dies, we have to determine why it died and whether
       or not we need to restart it.  This method manages that logic."""

    for col in all_living_collectors():
        now = int(time.time())
        # FIXME: this is not robust.  the asyncproc module joins on the
        # reader threads when you wait if that process has died.  this can cause
        # slow dying processes to hold up the main loop.  good for now though.
        status = col.proc.poll()
        if status is None:
            continue
        col.proc = None

        # behavior based on status.  a code 0 is normal termination, code 13
        # is used to indicate that we don't want to restart this collector.
        # any other status code is an error and is logged.
        if status == 13:
            LOG.info('removing %s from the list of collectors (by request)',
                      col.name)
            col.dead = True
        elif status != 0:
            LOG.warning('collector %s terminated after %d seconds with '
                        'status code %d, marking dead',
                        col.name, now - col.lastspawn, status)
            col.dead = True
        else:
            register_collector(Collector(col.name, col.interval, col.filename,
                                         col.mtime, col.lastspawn))

def check_children(options):
    """When a child process hasn't received a datapoint in a while,
       assume it's died in some fashion and restart it."""

    for col in all_living_collectors():
        now = int(time.time())

        if ((col.interval == 0) and (col.last_datapoint < (now - options.allowed_inactivity_time))):
            # It's too old, kill it
            LOG.warning('Terminating collector %s after %d seconds of inactivity',
                        col.name, now - col.last_datapoint)
            col.shutdown()
            if not options.remove_inactive_collectors:
                register_collector(Collector(col.name, col.interval, col.filename,
                                             col.mtime, col.lastspawn))


def set_nonblocking(fd):
    """Sets the given file descriptor to non-blocking mode."""
    fl = fcntl.fcntl(fd, fcntl.F_GETFL) | os.O_NONBLOCK
    fcntl.fcntl(fd, fcntl.F_SETFL, fl)


def spawn_collector(col):
    """Takes a Collector object and creates a process for it."""

    LOG.info('%s (interval=%d) needs to be spawned', col.name, col.interval)

    # FIXME: do custom integration of Python scripts into memory/threads
    # if re.search('\.py$', col.name) is not None:
    #     ... load the py module directly instead of using a subprocess ...
    try:
        col.proc = subprocess.Popen(col.filename, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    close_fds=True,
                                    preexec_fn=os.setsid)
    except OSError, e:
        LOG.error('Failed to spawn collector %s: %s' % (col.filename, e))
        return
    # The following line needs to move below this line because it is used in
    # other logic and it makes no sense to update the last spawn time if the
    # collector didn't actually start.
    col.lastspawn = int(time.time())
    # Without setting last_datapoint here, a long running check (>15s) will be 
    # killed by check_children() the first time check_children is called.
    col.last_datapoint = col.lastspawn
    set_nonblocking(col.proc.stdout.fileno())
    set_nonblocking(col.proc.stderr.fileno())
    if col.proc.pid > 0:
        col.dead = False
        LOG.info('spawned %s (pid=%d)', col.name, col.proc.pid)
        return
    # FIXME: handle errors better
    LOG.error('failed to spawn collector: %s', col.filename)


def spawn_children():
    """Iterates over our defined collectors and performs the logic to
       determine if we need to spawn, kill, or otherwise take some
       action on them."""

    if not ALIVE:
        return

    for col in all_valid_collectors():
        now = int(time.time())
        if col.interval == 0:
            if col.proc is None:
                spawn_collector(col)
        elif col.interval <= now - col.lastspawn:
            if col.proc is None:
                spawn_collector(col)
                continue

            # I'm not very satisfied with this path.  It seems fragile and
            # overly complex, maybe we should just reply on the asyncproc
            # terminate method, but that would make the main tcollector
            # block until it dies... :|
            if col.nextkill > now:
                continue
            if col.killstate == 0:
                LOG.warning('warning: %s (interval=%d, pid=%d) overstayed '
                            'its welcome, SIGTERM sent',
                            col.name, col.interval, col.proc.pid)
                kill(col.proc)
                col.nextkill = now + 5
                col.killstate = 1
            elif col.killstate == 1:
                LOG.error('error: %s (interval=%d, pid=%d) still not dead, '
                           'SIGKILL sent',
                           col.name, col.interval, col.proc.pid)
                kill(col.proc, signal.SIGKILL)
                col.nextkill = now + 5
                col.killstate = 2
            else:
                LOG.error('error: %s (interval=%d, pid=%d) needs manual '
                           'intervention to kill it',
                           col.name, col.interval, col.proc.pid)
                col.nextkill = now + 300


def populate_collectors(coldir):
    """Maintains our internal list of valid collectors.  This walks the
       collector directory and looks for files.  In subsequent calls, this
       also looks for changes to the files -- new, removed, or updated files,
       and takes the right action to bring the state of our running processes
       in line with the filesystem."""

    global GENERATION
    GENERATION += 1

    # get numerics from scriptdir, we're only setup to handle numeric paths
    # which define intervals for our monitoring scripts
    for interval in os.listdir(coldir):
        if not interval.isdigit():
            continue
        interval = int(interval)

        for colname in os.listdir('%s/%d' % (coldir, interval)):
            if colname.startswith('.'):
                continue

            filename = '%s/%d/%s' % (coldir, interval, colname)
            if os.path.isfile(filename) and os.access(filename, os.X_OK):
                mtime = os.path.getmtime(filename)

                # if this collector is already 'known', then check if it's
                # been updated (new mtime) so we can kill off the old one
                # (but only if it's interval 0, else we'll just get
                # it next time it runs)
                if colname in COLLECTORS:
                    col = COLLECTORS[colname]

                    # if we get a dupe, then ignore the one we're trying to
                    # add now.  there is probably a more robust way of doing
                    # this...
                    if col.interval != interval:
                        LOG.error('two collectors with the same name %s and '
                                   'different intervals %d and %d',
                                   colname, interval, col.interval)
                        continue

                    # we have to increase the generation or we will kill
                    # this script again
                    col.generation = GENERATION
                    if col.mtime < mtime:
                        LOG.info('%s has been updated on disk', col.name)
                        col.mtime = mtime
                        if not col.interval:
                            col.shutdown()
                            LOG.info('Respawning %s', col.name)
                            register_collector(Collector(colname, interval,
                                                         filename, mtime))
                else:
                    register_collector(Collector(colname, interval, filename,
                                                 mtime))

    # now iterate over everybody and look for old generations
    to_delete = []
    for col in all_collectors():
        if col.generation < GENERATION:
            LOG.info('collector %s removed from the filesystem, forgetting',
                      col.name)
            col.shutdown()
            to_delete.append(col.name)
    for name in to_delete:
        del COLLECTORS[name]


if __name__ == '__main__':
    sys.exit(main(sys.argv))
