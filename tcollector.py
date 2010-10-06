#!/usr/bin/python
# This file is part of tcollector.
# Copyright (C) 2010  StumbleUpon, Inc.
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
# Simple manager for collection scripts that run and gather data.  The tcollector
# gathers the data and sends it to the TSD for storage.
#
# by Mark Smith <msmith@stumbleupon.com>.
#

from __future__ import with_statement

import atexit
import errno
import fcntl
import logging
import os
import platform
import random
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from optparse import OptionParser


# global variables.
COLLECTORS = {}
GENERATION = 0
LOG = logging.getLogger('tcollector')
ALIVE = True


class Collector(object):
    """A Collector is a script that is run that gathers some data and prints it out in
       standard TSD format on STDOUT.  This class maintains all of the state information
       for a given collector and gives us utility methods for working with it."""

    def __init__(self, colname, interval, filename, mtime=0, lastspawn=0):
        """Construct a new Collector.  This also installs the new collector into the global
           dictionary that stores all of the collectors."""
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
        self.values = {}
        self.lines_sent = 0
        self.lines_received = 0
        self.lines_invalid = 0

        # store us in the global list and initiate a kill for anybody with our name that
        # happens to still be hanging around
        if colname in COLLECTORS:
            col = COLLECTORS[colname]
            if col.proc is not None:
                LOG.error('%s still has a process (pid=%d) and is being reset,'
                          ' terminating', col.name, col.proc.pid)
                kill(col.proc)

        COLLECTORS[colname] = self

    def read(self):
        """Read bytes from our subprocess and store them in our temporary line storage
           buffer.  This needs to be non-blocking."""

        # now read stderr for log messages, we could buffer here but since we're just
        # logging the messages, I don't care to
        try:
            out = self.proc.stderr.read()
            if out:
                LOG.debug('reading %s got %d bytes on stderr', self.name, len(out))
                for line in out.splitlines():
                    LOG.warning('%s: %s', self.name, line)
        except IOError, (err, msg):
            if err != errno.EAGAIN:
                raise
        except:
            LOG.exception('uncaught exception in stderr read')

        # we have to use a buffer because sometimes the collectors will write out a bunch
        # of data points at one time and we get some weird sized chunk.  This read call
        # is non-blocking.
        try:
            self.buffer += self.proc.stdout.read()
            if len(self.buffer):
                LOG.debug('reading %s, buffer now %d bytes', self.name, len(self.buffer))
        except IOError, (err, msg):
            if err != errno.EAGAIN:
                raise
        except:
            # sometimes the process goes away in another thread and we don't have it
            # anymore, so log an error and bail
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
            self.buffer = self.buffer[idx+1:]

    def collect(self):
        """Reads input from the collector and returns the lines up to whomever is calling us.
           This is a generator that returns a line as it becomes available."""

        while self.proc is not None:
            self.read()
            if not len(self.datalines):
                return
            while len(self.datalines):
                yield self.datalines.pop(0)


class StdinCollector(Collector):
    """A StdinCollector simply reads from STDIN and provides the data.  This collector
       helps ensure we are always reading so that we don't block and presents a uniform
       interface for the ReaderThread."""

    def __init__(self, options, modules, sender, tags):
        Collector.__init__(self, 'stdin', 0, '<stdin>')
        self.options = options
        self.modules = modules
        self.sender = sender
        self.tags = tags

        # hack to make this work.  nobody else will rely on self.proc except as a test
        # in the stdin mode.
        self.proc = True

        # make stdin a non-blocking file
        fd = sys.stdin.fileno()
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

    def read(self):
        """Read lines from STDIN and store them.  We allow this to be blocking because
           there should only ever be one StdinCollector and if we're using it that means
           we have no normal collectors (can't do both) so the ReaderThread is only
           serving us and we're allowed to block it."""

        ts = int(time.time())
        while True:
            try:
                line = sys.stdin.readline()
            except IOError:
                break

            # The only time that line comes back as empty is when we get an
            # EOF (blank) from readline.  Every other case at least gives us a
            # \n, which won't match this condition.
            if not line:
                global ALIVE
                ALIVE = False
                break

            line = line.rstrip()
            if line:
                self.datalines.append(line)
            newts = int(time.time())
            if newts > ts + 15:
                reload_changed_config_modules(modules, options, self.sender,
                                              tags)
                ts = newts


class ReaderThread(threading.Thread):
    """The main ReaderThread is responsible for reading from the collectors and
       assuring that we always read from the input no matter what is going on with the
       output process.  This ensures we don't block."""

    def __init__(self):
        threading.Thread.__init__(self)

        self.outq = []
        self.tempq = []
        self.outqlock = threading.Lock()
        self.ready = threading.Event()
        self.lines_collected = 0
        self.lines_dropped = 0

    def run(self):
        """Main loop for this thread.  Just reads from collectors, does our input processing and
           de-duping, and puts the data into the global queue."""

        LOG.debug("ReaderThread up and running")

        # we loop every second for now.  ideally we'll setup some select or other
        # thing to wait for input on our children, while breaking out every once in a
        # while to setup selects on new children.
        while ALIVE:
            # this should be entirely non-blocking and go fast
            for col in all_living_collectors():
                for line in col.collect():
                    self.process_line(col, line)

                    # every once in a while, break out.  this ensures that we don't read
                    # forever on a collector that is trying its best to spam us (like a
                    # stdin collector piping from a file)
                    if len(self.tempq) > 1024:
                        break

            # if we have lines, get the log and copy them
            if len(self.tempq):
                with self.outqlock:
                    self.lines_collected += len(self.tempq)
                    for line in self.tempq:
                        self.outq.append(line)

                    # if we now have more lines than we care to, we need to start dropping older
                    # ones... this is unfortunate but it's better than bloating forever.
                    # FIXME: spool to disk or some other storage mechanism so we don't lose them?
                    if len(self.outq) > 100000:
                        LOG.error('outbound queue trimmed down from %d lines', len(self.outq))
                        self.lines_dropped += len(self.outq) - 100000
                        self.outq = self.outq[-100000:]

                    LOG.debug('ReaderThread.outq now has %d lines', len(self.outq))

                self.tempq = []
                self.ready.set()
                continue
            else:
                self.ready.clear()

            # and here is the loop that we really should get rid of, this just prevents
            # us from spinning right now
            time.sleep(1)

    def process_line(self, col, line):
        """Parses the given line and appends the result to the internal queue."""

        col.lines_received += 1
        parsed = re.match('^([-_.a-zA-Z0-9]+)\s+'  # Metric name.
                          '(\d+)\s+'               # Timestamp.
                          '(\S+?)'                 # Value (int or float).
                          '((?:\s+[-_.a-zA-Z0-9]+=[-_.a-zA-Z0-9]+)*)$', line)  # Tags.
        if parsed is None:
            LOG.warning('%s sent invalid data: %s', col.name, line)
            col.lines_invalid += 1
            return
        metric, timestamp, value, tags = parsed.groups()

        # De-dupe detection...  This reduces the noise we send to the TSD so
        # we don't store data points that don't change.  This is a hack, as
        # it can be defeated by sending tags in different orders...  But that's
        # probably OK.
        key = (metric, tags)
        if key in col.values:
            # if the timestamp didn't do what we expected, ignore this value
            if timestamp <= col.values[key][3]:
                LOG.error("Timestamp unexpected: metric=%s %s, old_ts=%d, new_ts=%d.",
                        metric, tags, col.values[key][3], timestamp)
                return

            # if this data point is repeated, store it but don't send
            if col.values[key][0] == value:
                col.values[key] = (value, True, line, timestamp)
                return

            # we might have to append two lines if the value has been the same for a while
            # and we've skipped one or more values.  we need to replay the last value
            # we skipped so the jumps in our graph are accurate.
            if col.values[key][1]:
                col.lines_sent += 1
                self.tempq.append(col.values[key][2])

        # now we can reset for the next pass and send the line we actually want to send
        col.values[key] = (value, False, line, timestamp)
        col.lines_sent += 1
        self.tempq.append(line)


class SenderThread(threading.Thread):
    """The SenderThread is responsible for maintaining a connection to the TSD and sending
       the data we're getting over to it.  This thread is also responsible for doing any
       sort of emergency buffering we might need to do if we can't establish a connection
       and we need to spool to disk.  That isn't implemented yet."""

    def __init__(self, reader, dryrun, host, port, tags):
        threading.Thread.__init__(self)

        self.dryrun = dryrun
        self.host = host
        self.port = port
        self.reader = reader
        self.tagstr = tags
        self.tsd = None
        self.last_verify = 0
        self.outq = []

    def run(self):
        """Main loop.  This just blocks on the ReaderThread to have data for us and when
           it has data we try to send it."""

        while ALIVE:
            self.reader.ready.wait()
            self.maintain_conn()
            with self.reader.outqlock:
                for line in self.reader.outq:
                    self.outq.append(line)
                self.reader.outq = []
            self.send_data()

    def verify_conn(self):
        if self.tsd is None:
            return False

        # if the last verification was less than a minute ago, don't re-verify
        if self.last_verify > time.time() - 60:
            return True

        # we use the version command as it is very low effort for the TSD to respond
        LOG.debug('verifying our TSD connection is alive')
        try:
            self.tsd.sendall('version\n')
        except socket.error, msg:
            self.tsd = None
            return False

        while True:
            # try to read as much data as we can.  at some point this is going to
            # block, but we have set the timeout low when we made the connection
            try:
                buf = self.tsd.recv(4096)
            except socket.error, msg:
                self.tsd = None
                return False

            # if we get data... then everything looks good
            if len(buf):
                # and if everything is good, send out our meta stats.  this helps to see
                # what is going on with the tcollector
                if len(buf) < 4096:
                    strs = [
                            ('reader.lines_collected', '', self.reader.lines_collected),
                            ('reader.lines_dropped', '', self.reader.lines_dropped)
                           ]

                    for col in all_living_collectors():
                        strs.append(('collector.lines_sent', 'collector=' + col.name, col.lines_sent))
                        strs.append(('collector.lines_received', 'collector=' + col.name, col.lines_received))
                        strs.append(('collector.lines_invalid', 'collector=' + col.name, col.lines_invalid))

                    ts = int(time.time())
                    strout = ["tcollector.%s %d %d %s" % (x[0], ts, x[2], x[1]) for x in strs]
                    for str in strout:
                        self.outq.append(str)
                    break
            else:
                self.tsd = None
                return False

        # if we get here, we assume the connection is good
        self.last_verify = time.time()
        return True

    def maintain_conn(self):
        """Safely connect to the TSD and ensure that it's up and running and that we're not
           talking to a ghost connection (no response)."""

        # dry runs are always good
        if self.dryrun:
            return

        # connection didn't verify, so create a new one.  we might be in this method for
        # a long time while we sort this out.
        try_delay = 1
        while True:
            if self.verify_conn():
                return

            # increase the try delay by some amount and some random value, in case
            # the TSD is down for a while.  delay at most approximately 10 minutes.
            try_delay *= 1 + random.random()
            if try_delay > 600:
                try_delay *= 0.5
            LOG.debug('SenderThread blocking %0.2f seconds', try_delay)
            time.sleep(try_delay)

            # now actually try the connection
            try:
                self.tsd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.tsd.settimeout(15)
                self.tsd.connect((self.host, self.port))
            except socket.error, msg:
                LOG.error('failed to connect to %s:%d: %s', self.host, self.port, msg)
                self.tsd.close()
                self.tsd = None

    def send_data(self):
        """Sends outstanding data to the TSD in one operation."""

        # construct the output string
        out = ''
        for line in self.outq:
            line = 'put ' + line + self.tagstr
            out += line + '\n'
            LOG.debug('SENDING: %s' % line)

        # try sending our data.  if an exception occurs, just error and try sending
        # again next time.
        try:
            if self.dryrun:
                print out
            else:
                self.tsd.sendall(out)
            self.outq = []
        except socket.error, msg:
            LOG.error('failed to send data: %s', msg)
            try:
                self.tsd.close()
            except socket.error:
                pass
            self.tsd = None

        # FIXME: we should be reading the result at some point to drain the packets
        # out of the kernel's queue


def main(argv):
    """The main tcollector entry point and loop."""

    LOG.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(message)s'))
    LOG.addHandler(ch)

    # get arguments
    parser = OptionParser(description='Manages collectors which gather data and report back.')
    parser.add_option('-c', '--collector-dir', dest='cdir', default='./collectors', metavar='DIR',
            help='Directory where the collectors are located.')
    parser.add_option('-d', '--dry-run', dest='dryrun', action='store_true', default=False,
            help='Don\'t actually send anything to the TSD, just print the datapoints.')
    parser.add_option('-H', '--host', dest='host', default='localhost', metavar='HOST',
            help='Hostname to use to connect to the TSD.')
    parser.add_option('-s', '--stdin', dest='stdin', action='store_true', default=False,
            help='Run once, read and dedup data points from stdin.')
    parser.add_option('-p', '--port', dest='port', type='int', default=4242, metavar='PORT',
            help='Port to connect to the TSD instance on.')
    parser.add_option('-v', dest='verbosity', action='count', default=0,
            help='Verbose mode.  Specify twice for debugging output.')
    parser.add_option('-t', '--tag', dest='tags', action='append', default=[], metavar='TAG',
            help='Tags to append to all timeseries we send, e.g.: -t TAG=VALUE -t TAG2=VALUE')
    parser.add_option('-P', '--pidfile', dest='pidfile', default='/var/run/tcollector.pid',
            metavar='FILE', help='Write our pidfile')
    (options, args) = parser.parse_args(args=argv[1:])

    if options.verbosity > 1:
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

    options.cdir = os.path.realpath(options.cdir)
    if not os.path.isdir(options.cdir):
        LOG.fatal('No such directory: %s', options.cdir)
        return 1
    modules = load_etc_dir(options, tags)

    # tsdb does not require a host tag, but we do.  we are always running on a
    # host.  FIXME: we should make it so that collectors may request to set their
    # own host tag, or not set one.
    if not 'host' in tags and not options.stdin:
        tags['host'] = socket.gethostname()
        LOG.warning('Tag "host" not specified, defaulting to %s.', tags['host'])

    # prebuild the tag string from our tags dict
    tagstr = ''
    if tags:
        tagstr = ' '.join('%s=%s' % (k, v) for k, v in tags.iteritems())
        tagstr = ' ' + tagstr.strip()

    # gracefully handle death for normal termination paths and abnormal
    atexit.register(shutdown)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, shutdown_signal)

    # at this point we're ready to start processing, so start the ReaderThread so we can
    # have it running and pulling in data for us
    rdr = ReaderThread()
    rdr.start()

    # and setup the sender to start writing out to the tsd
    sender = SenderThread(rdr, options.dryrun, options.host, options.port,
                          tagstr)
    sender.start()
    LOG.info('thread startup complete')

    # if we're in stdin mode, build a stdin collector and just join on the reader thread
    # since there's nothing else for us to do here
    if options.stdin:
        StdinCollector(options, modules, sender, tags)
        while ALIVE:
            # Thread.join() is completely blocking and will prevent signal
            # handlers from running.  So instead we try to join the thread
            # every second.  This way, signal handlers get a chance to run.
            rdr.join(1.0)
    else:
        main_loop(options, modules, sender, tags)


def main_loop(options, modules, sender, tags):
    """The main loop of the program that runs when we're not in stdin mode."""

    while True:
        populate_collectors(options.cdir)
        reload_changed_config_modules(modules, options, sender, tags)
        reap_children()
        spawn_children()
        time.sleep(15)


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

    if changed:
        sender.tagstr = ' ' + ' '.join('%s=%s' % (k, v)
                                       for k, v in tags.iteritems())
        sender.tagstr = sender.tagstr.strip()
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
    """Generator to return all defined collectors that haven't been marked dead (e.g. by the
       collector process returning exit status 13)."""

    for col in all_collectors():
        if not col.dead:
            yield col


# collectors that have a process attached (currenty alive)
def all_living_collectors():
    """Generator to return all defined collectors that have an active process."""

    for col in all_collectors():
        if col.proc is not None:
            yield col


def shutdown_signal(signum, frame):
    """Called when we get a signal and need to terminate."""
    LOG.warning("shutting down, got signal %d", signum)
    shutdown()


def kill(proc, signum=signal.SIGTERM):
  os.kill(proc.pid, signum)


def shutdown():
    """Called by atexit and when we receive a signal, this ensures we properly terminate
       any outstanding children."""

    LOG.info('shutting down children')

    # tell everyone to die
    for col in all_living_collectors():
        try:
            if col.proc.poll() is None:
                kill(col.proc)
                col.proc.wait()
        except:
            # we really don't want to die as we're trying to exit gracefully
            LOG.exception('ignoring uncaught exception while shutting down')
            continue

    LOG.info('exiting')
    sys.exit(1)


def reap_children():
    """When a child process dies, we have to determine why it died and whether or not
       we need to restart it.  This method manages that logic."""

    for col in all_living_collectors():
        now = int(time.time())
        # FIXME: this is not robust.  the asyncproc module joins on the
        # reader threads when you wait if that process has died.  this can cause
        # slow dying processes to hold up the main loop.  good for now though.
        status = col.proc.poll()
        if status is None:
            continue
        col.proc = None

        # behavior based on status.  a code 0 is normal termination, code 13 is used
        # to indicate that we don't want to restart this collector.  any other status
        # code is an error and is logged.
        if status == 13:
            LOG.info('removing %s from the list of collectors (by request)', col.name)
            col.dead = True
        elif status != 0:
            LOG.warning('collector %s terminated after %d seconds with status code %d, marking dead',
                    col.name, now - col.lastspawn, status)
            col.dead = True
        else:
            Collector(col.name, col.interval, col.filename, col.mtime, col.lastspawn)


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
    col.lastspawn = int(time.time())
    col.proc = subprocess.Popen(col.filename, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
    set_nonblocking(col.proc.stdout.fileno())
    set_nonblocking(col.proc.stderr.fileno())
    if col.proc.pid > 0:
        LOG.info('spawned %s (pid=%d)', col.name, col.proc.pid)
        return
    # FIXME: handle errors better
    LOG.error('failed to spawn collector: %s', col.filename)


def spawn_children():
    """Iterates over our defined collectors and performs the logic to determine if we need
       to spawn, kill, or otherwise take some action on them."""

    for col in all_valid_collectors():
        now = int(time.time())
        if col.interval == 0:
            if col.proc is None:
                spawn_collector(col)
        elif col.interval <= now - col.lastspawn:
            if col.proc is None:
                spawn_collector(col)
                continue

            # I'm not very satisfied with this path.  It seems fragile and overly complex, maybe
            # we should just reply on the asyncproc terminate method, but that would make the main
            # tcollector block until it dies... :|
            if col.nextkill > now:
                continue
            if col.killstate == 0:
                LOG.warning('warning: %s (interval=%d, pid=%d) overstayed its welcome, SIGTERM sent',
                        col.name, col.interval, col.proc.pid)
                kill(col.proc)
                col.nextkill = now + 5
                col.killstate = 1
            elif col.killstate == 1:
                LOG.error('error: %s (interval=%d, pid=%d) still not dead, SIGKILL sent',
                        col.name, col.interval, col.proc.pid)
                kill(col.proc, signal.SIGKILL)
                col.nextkill = now + 5
                col.killstate = 2
            else:
                LOG.error('error: %s (interval=%d, pid=%d) needs manual intervention to kill it',
                        col.name, col.interval, col.proc.pid)
                col.nextkill = now + 300


def populate_collectors(coldir):
    """Maintains our internal list of valid collectors.  This walks the collector
       directory and looks for files.  In subsequent calls, this also looks for
       changes to the files -- new, removed, or updated files, and takes the right
       action to bring the state of our running processes in line with the filesystem."""

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
            if os.path.isfile(filename):
                mtime = os.path.getmtime(filename)

                # if this collector is already 'known', then check if it's been updated (new mtime)
                # so we can kill off the old one (but only if it's interval 0, else we'll just get
                # it next time it runs)
                if colname in COLLECTORS:
                    col = COLLECTORS[colname]

                    # if we get a dupe, then ignore the one we're trying to add now.  there is probably a
                    # more robust way of doing this...
                    if col.interval != interval:
                        LOG.error('two collectors with the same name %s and different intervals %d and %d',
                                colname, interval, col.interval)
                        continue

                    # we have to increase the generation or we will kill this script again
                    col.generation = GENERATION
                    if col.proc is not None and not col.interval and col.mtime < mtime:
                        LOG.info('%s has been updated on disk, respawning', col.name)
                        col.mtime = mtime
                        kill(col.proc)
                else:
                    Collector(colname, interval, filename, mtime)

    # now iterate over everybody and look for old generations
    to_delete = []
    for col in all_collectors():
        if col.generation < GENERATION:
            LOG.info('collector %s removed from the filesystem, forgetting', col.name)
            if col.proc is not None:
                kill(col.proc)
            to_delete.append(col.name)
    for name in to_delete:
        del COLLECTORS[name]


if __name__ == '__main__':
    sys.exit(main(sys.argv))
