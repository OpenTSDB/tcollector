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

import atexit
import errno
import fcntl
import logging
import os
import platform
import re
import signal
import socket
import subprocess
import sys
import time
from optparse import OptionParser


# global variables.
GENERATION = 0
LOG = logging.getLogger('tcollector')
COLLECTORS = {}
OUTQ = []
TSD = None


class Collector(object):
    """A Collector is a script that is run that gathers some data and prints it out in
       standard TSD format on STDOUT.  This class maintains all of the state information
       for a given collector and gives us utility methods for working with it."""

    pass


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
    parser.add_option('-H', '--host', dest='host', default='localhost', metavar='HOST',
            help='Hostname to use to connect to the TSD.')
    parser.add_option('-p', '--port', dest='port', type='int', default=4242, metavar='PORT',
            help='Port to connect to the TSD instance on.')
    parser.add_option('-v', dest='verbosity', action='count', default=0,
            help='Verbose mode.  Specify twice for debugging output.')
    parser.add_option('-t', '--tag', dest='tags', action='append', default=[], metavar='TAG',
            help='Tags to append to all timeseries we send, e.g.: -t TAG=VALUE -t TAG2=VALUE')
    (options, args) = parser.parse_args(args=argv[1:])

    if options.verbosity > 1:
        LOG.setLevel(logging.DEBUG)  # up our level

    # validate everything
    tags = {}
    for tag in options.tags:
        if re.match('^[-_.a-z0-9]+=\S+$', tag, re.IGNORECASE) is None:
            assert False, 'Tag string "%s" is invalid.' % tag
        k, v = tag.split('=', 1)
        if k in tags:
            assert False, 'Tag "%s" already declared.' % k
        tags[k] = v

    # tsdb does not require a host tag, but we do.  we are always running on a
    # host.  FIXME: we should make it so that collectors may request to set their
    # own host tag, or not set one.
    if not 'host' in tags:
        tags['host'] = socket.gethostname()
        LOG.warning('Tag "host" not specified, defaulting to %s.', tags['host'])

    # prebuild the tag string from our tags dict
    tagstr = ' ' + ' '.join('%s=%s' % (k, v) for k, v in tags.iteritems())

    # gracefully handle death for normal termination paths and abnormal
    atexit.register(shutdown)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, shutdown_signal)

    # startup complete
    LOG.info('startup complete')

    # main loop to handle all of our jobs
    # FIXME: there are more efficient ways of doing this than just looping every second
    while True:
        populate_collectors(options.cdir)
        read_children()
        reap_children()
        spawn_children()
        send_to_tsd(options, tagstr)
        time.sleep(1)


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
        if col.proc.poll() is None:
            kill(col.proc)
            col.proc.wait()

    LOG.info('exiting')
    sys.exit(1)


def connect_to_tsd(options):
    """Safely connect to the TSD."""

    global TSD

    try:
        TSD = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        TSD.settimeout(15)
        TSD.connect((options.host, options.port))
    except socket.error, msg:
        LOG.error('failed to connect to %s:%d: %s', options.host, options.port, msg)
        TSD.close()
        TSD = None


def send_to_tsd(options, tagstr):
    """Sends outstanding data to the TSD in one operation."""

    global OUTQ, TSD
    if len(OUTQ) == 0:
        return

    # if our TSD socket isn't built, do that now.  if we can't connect, make sure
    # we don't continue to the path below.
    if TSD is None:
        connect_to_tsd(options)
        if TSD is None:
            return

    out = ''
    for line in OUTQ:
        line = 'put ' + line + tagstr
        out += line + '\n'
        LOG.debug('SENDING: %s' % line)

    # try sending our data.  if an exception occurs, just error and try sending
    # again next time.
    try:
        TSD.sendall(out)
        OUTQ = []
    except socket.error, msg:
        LOG.error('failed to send data: %s', msg)
        TSD = None


def read_children():
    """Iterates over all of our children with processes and reads and handles data
       that they have sent via STDOUT.  This is where we do any special processing of
       the client data such as commands or special arguments."""

    now = int(time.time())
    for col in all_living_collectors():
        # now read stderr for log messages
        try:
            out = col.proc.stderr.read()
        except IOError, (err, msg):
            if err == errno.EAGAIN:
                continue
            raise
        if out:
            LOG.debug('reading %s got %d bytes on stderr', col.name, len(out))
            for line in out.splitlines():
                LOG.warning('%s: %s', col.name, line)

        # FIXME: this should take the read data and append it onto a buffer and then
        # we should pull off complete lines from the front of the buffer.  right now,
        # the following code doesn't handle a situation where we get a partial line in
        # the read.
        try:
            out = col.proc.stdout.read()
        except IOError, (err, msg):
            if err == errno.EAGAIN:
                continue
            raise
        if not out:
            continue
        LOG.debug('read %s got %d bytes', col.name, len(out))
        for line in out.splitlines():
            if re.match('^[-_.a-z0-9]+\s+\d+\s+\S+?(\s+[-_.a-z0-9]+=[-_.a-z0-9]+)*$', line) is None:
                LOG.warning('%s sent invalid data: %s', col.name, line)
            else:
                OUTQ.append(line)


def reap_children():
    """When a child process dies, we have to determine why it died and whether or not
       we need to restart it.  This method manages that logic."""

    now = int(time.time())
    for col in all_living_collectors():
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
        else:
            if status != 0:
                LOG.warning('collector %s terminated after %d seconds with status code %d',
                        col.name, now - col.lastspawn, status)
            reset_collector(col.interval, col.name, col.filename, col.mtime, col.lastspawn)


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

    now = int(time.time())
    for col in all_valid_collectors():
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


def reset_collector(interval, colname, filename, mtime=0, lastspawn=0):
    """Builds a new Collector object.  If one already exists with the given name, this
       will terminate it and then rebuild it."""

    # if this collector already exists, then make sure we kill it
    if colname in COLLECTORS:
        col = COLLECTORS[colname]
        if col.proc is not None:
            LOG.error('%s still has a process (pid=%d) and is being reset, terminating',
                      col.name, col.proc.pid)
            kill(col.proc)

    col = Collector()
    col.name = colname
    col.interval = interval
    col.filename = filename
    col.lastspawn = lastspawn
    col.proc = None
    col.nextkill = 0
    col.killstate = 0
    col.dead = False
    col.mtime = mtime
    col.generation = GENERATION

    COLLECTORS[colname] = col


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
                    reset_collector(interval, colname, filename, mtime)

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
    main(sys.argv)
