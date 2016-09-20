#!/usr/bin/env python

import os
import signal
import logging
import sys
import re
import socket
import time
import ConfigParser
import imp
import json
import urllib2
import random
import base64
import threading
from optparse import OptionParser
from Queue import Queue
from Queue import Full
from Queue import Empty
import ssl
import common_utils

# global variables._
COLLECTORS = {}
DEFAULT_LOG = '/var/log/cloudwiz-collector.log'
LOG = logging.getLogger('runner')
# TODO consider put into config file
DEFAULT_PORT = 4242
ALLOWED_INACTIVITY_TIME = 600  # seconds
MAX_UNCAUGHT_EXCEPTIONS = 100
MAX_SENDQ_SIZE = 20000      # this should match tsd.http.request.max_chunk, usually 1/3. json adds considerable overhead
MAX_READQ_SIZE = 100000

# config constants
SECTION_BASE = 'base'
CONFIG_ENABLED = 'enabled'
CONFIG_COLLECTOR_CLASS = 'collectorclass'
CONFIG_INTERVAL = 'interval'

# metric entry constant
METRIC_NAME = 'metric'
METRIC_TIMESTAMP = 'timestamp'
METRIC_VALUE = 'value'
METRIC_TAGS = 'tags'


def main(argv):
    try:
        options, args = parse_cmdline(argv)
    except:
        sys.stderr.write("Unexpected error: %s" % sys.exc_info()[0])
        return 1

    if options.daemonize:
        daemonize()

    common_utils.setup_logging(LOG, options.logfile, options.max_bytes or None, options.backup_count or None)

    if options.verbose:
        LOG.setLevel(logging.DEBUG)  # up our level

    LOG.info('agent starting..., %s', argv)

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

    if 'host' not in tags and not options.stdin:
        tags['host'] = socket.gethostname()
        LOG.warning('Tag "host" not specified, defaulting to %s.', tags['host'])

    options.cdir = os.path.realpath(options.cdir)
    if not os.path.isdir(options.cdir):
        LOG.fatal('No such directory: %s', options.cdir)
        return 1

    setup_python_path(options.cdir)

    # gracefully handle death for normal termination paths and abnormal
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, shutdown_signal)

    # prepare list of (host, port) of TSDs given on CLI
    if not options.hosts:
        options.hosts = [(options.host, options.port)]
    else:
        def splithost(hostport):
            if ":" in hostport:
                # Check if we have an IPv6 address.
                if hostport[0] == "[" and "]:" in hostport:
                    host, port = hostport.split("]:")
                    host = host[1:]
                else:
                    host, port = hostport.split(":")
                return host, int(port)
            return hostport, DEFAULT_PORT

        options.hosts = [splithost(host_str) for host_str in options.hosts.split(",")]
        if options.host != "localhost" or options.port != DEFAULT_PORT:
            options.hosts.append((options.host, options.port))

    runner_config = load_runner_conf()
    token = runner_config.get('base', 'token')

    readq = NonBlockingQueue(MAX_READQ_SIZE)
    global SENDER
    SENDER = Sender(token, readq, options, tags)
    SENDER.start()

    LOG.info('agent finish initializing, enter main loop.')
    main_loop(readq, options, {}, COLLECTORS)


def main_loop(readq, options, configs, collectors):
    loop_interval = options.update_interval
    while True:
        start = time.time()
        try:
            changed_configs, deleted_configs = reload_collector_confs(configs, options)
            close_collecotors(deleted_configs, collectors)
            load_collectors(options.cdir, changed_configs, collectors, readq)
            if options.verbose:
                import datetime
                sys.stdout.write('sent data at %s\n' % datetime.datetime.fromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S'))
        except:
            LOG.exception('failed collector update loop.')

        end = time.time()
        sleep_reasonably(loop_interval, start, end)


def load_runner_conf():
    runner_config_path = os.path.splitext(__file__)[0] + ".conf"
    runner_config = ConfigParser.SafeConfigParser()
    runner_config.read(runner_config_path)
    return runner_config


def reload_collector_confs(collector_confs, options):
    """
    load and reload collector conf file
    Args:
        collector_confs:
        options:

    Returns: changed collector confs and deleted collector confs

    """
    confdir = os.path.join(options.cdir, 'conf')
    current_collector_confs = set(list_collector_confs(confdir))
    changed_collector_confs = {}
    deleted_collector_confs = {}

    # Reload any module that has changed.
    for filename, (path, conf, timestamp) in collector_confs.iteritems():
        if filename not in current_collector_confs:  # Module was removed.
            continue
        mtime = os.path.getmtime(path)
        if mtime > timestamp:
            LOG.info('reloading %s, file has changed', path)
            config = ConfigParser.SafeConfigParser(default_config())
            config.read(path)
            collector_confs[filename] = (path, config, mtime)
            changed_collector_confs[filename] = (path, config, mtime)

    # Remove any module that has been removed.
    for filename in set(collector_confs).difference(current_collector_confs):
        LOG.info('%s has been removed, agent should be restarted', filename)
        deleted_collector_confs[filename] = collector_confs[filename]
        del collector_confs[filename]

    # Check for any modules that may have been added.
    for filename in current_collector_confs:
        path = os.path.join(confdir, filename)
        if filename not in collector_confs:
            LOG.info('adding conf %s', path)
            config = ConfigParser.SafeConfigParser(default_config())
            config.read(path)
            collector_confs[filename] = (path, config, os.path.getmtime(path))
            changed_collector_confs[filename] = (path, config, os.path.getmtime(path))
    return changed_collector_confs, deleted_collector_confs


def default_config():
    return {
        CONFIG_ENABLED: 'False',
        CONFIG_INTERVAL: '15',
        CONFIG_COLLECTOR_CLASS: None
    }


def list_collector_confs(confdir):
    if not os.path.isdir(confdir):
        LOG.warn('collector conf directory %s is not a directory', confdir)
        return iter(())  # Empty iterator.
    return (name for name in os.listdir(confdir)
            if (name.endswith('.conf') and os.path.isfile(os.path.join(confdir, name))))


def close_collecotors(configs, collectors):
    for config_filename in configs.keys():
        name = os.path.splitext(config_filename)[0]
        close_single_collector(collectors, name)


def close_single_collector(collectors, name):
    try:
        LOG.info('shutting down collector % that has been removed in config file', name)
        collectors[name].shutdown()
        del collectors[name]
    except:
        LOG.error('failed to shutdown collector %s', name)


def load_collectors(coldir, configs, collectors, readq):
    collector_dir = '%s/builtin' % coldir
    for config_filename, (path, conf, timestamp) in configs.iteritems():
        try:
            name = os.path.splitext(config_filename)[0]
            collector_path_name = '%s/%s.py' % (collector_dir, name)
            if conf.getboolean(SECTION_BASE, CONFIG_ENABLED):
                if os.path.isfile(collector_path_name) and os.access(collector_path_name, os.X_OK):
                    collector_class_name = conf.get(SECTION_BASE, CONFIG_COLLECTOR_CLASS)
                    collector_class = load_collector_module(name, collector_dir, collector_class_name)
                    collector_instance = collector_class(conf, LOG, readq)
                    interval = conf.getint(SECTION_BASE, CONFIG_INTERVAL)

                    # shutdown and remove old collector
                    if name in collectors:
                        close_single_collector(collectors, name)
                    collectors[name] = CollectorExec(name, collector_instance, interval)
                    LOG.info('loaded collector %s from %s', name, collector_path_name)
                else:
                    LOG.warn('failed to access collector file: %s', collector_path_name)
            elif name in collectors:
                LOG.info("%s is disabled, shut down and remove it", collector_path_name)
                close_single_collector(collectors, name)
        except:
            LOG.exception('failed to load collector %s, skipped.', collector_path_name if collector_path_name else config_filename)


# caller to handle exception
def load_collector_module(module_name, module_path, collector_class_name=None):
    (file_obj, filename, description) = imp.find_module(module_name, [module_path])
    mod = imp.load_module(module_name, file_obj, filename, description)
    if collector_class_name is None:
        collector_class_name = module_name.title().replace('_', '').replace('-', '')
    return getattr(mod, collector_class_name)


def parse_cmdline(argv):

    try:
        defaults = get_defaults()
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
    parser.add_option('--reconnect-interval', dest='reconnectinterval', type='int',
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
    parser.add_option('--update-interval', dest='update_interval', type='int', default=defaults['update_interval'],
                      help='interval the update of collector is picked up')
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
    return options, args


def get_defaults():
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
        'hosts': False,
        'update_interval': 15
    }

    return defaults


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
            pass  # ... ignore the exception.


def write_pid(pidfile):
    """Write our pid to a pidfile."""
    f = open(pidfile, "w")
    try:
        f.write(str(os.getpid()))
    finally:
        f.close()


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


def shutdown():
    LOG.info('exiting...')
    SENDER.shutdown()
    for name, collector in COLLECTORS.iteritems():
        try:
            # there are collectors spawning subprocesses (shell top), we need to shut them down cleanly
            collector.signal_shutdown()
        except:
            LOG.exception('failed to signal shutdown collector %s. skip.', name)

    for name, collector in COLLECTORS.iteritems():
        try:
            # there are collectors spawning subprocesses (shell top), we need to shut them down cleanly
            collector.wait_shutdown()
        except:
            LOG.exception('failed to wait shutdown collector %s. skip.', name)

    LOG.info('total %d collectors exited', len(COLLECTORS))
    sys.exit(1)


# noinspection PyUnusedLocal
def shutdown_signal(signum, frame):
    LOG.warning("shutting down, got signal %d", signum)
    shutdown()


def sleep_reasonably(interval, start, end):
    sleepsec = interval - (end - start) if interval > (end - start) else 0
    time.sleep(sleepsec)


class CollectorExec(object):
    def __init__(self, name, collector_instance, interval):
        self._validate(name, 'name')
        self._validate(collector_instance, 'collector_instance')
        self._validate(interval, 'interval')

        self._name = name
        self._collector_instance = collector_instance
        self._interval = interval
        self._thread = CollectorThread(name, collector_instance, interval)
        self._thread.start()

    def shutdown(self, wait=True):
        LOG.info('starting to shut down %s', self._name)
        if self._collector_instance:
            self._collector_instance.signal_exit()
        else:
            LOG.error('no collector instance for %s', self._name)

        if self._thread:
            self._thread.exit = True
        else:
            LOG.error('no thread instance for %s', self._name)
        if wait:
            self._thread.join()
            LOG.info('finish shutting down %s', self._name)

    def signal_shutdown(self):
        """ signal shutdown without waiting for the thread to exit, should used in pair with wait_shutdown"""
        self.shutdown(False)

    def wait_shutdown(self):
        """ used in pair with signal_shutdown to wait for the thread to exit """
        self._thread.join()
        LOG.info('finish shutting down %s', self._name)

    def _validate(self, val, name):
        if not val:
            raise ValueError('%s is not set' % name)


class CollectorThread(threading.Thread):
    def __init__(self, name, collector_instance, interval):
        super(CollectorThread, self).__init__()
        self.name = name
        self.collector_instance = collector_instance
        self.interval = interval
        self.exit = False

    def run(self):
        LOG.info('started collector thread: %s', self.name)
        while not self.exit:
            start = time.time()
            try:
                LOG.info("start one collection for collector %s", self.name)
                self.collector_instance()
                LOG.info("finish one collection for collector %s", self.name)
            except:
                LOG.exception('failed to execute collector %s', self.name)
            self.sleep_responsively(start)
        self.collector_instance.cleanup()

    def sleep_responsively(self, start):
        max_sleepsec = 5
        end = time.time()
        sleepsec = self.interval - (end - start) if self.interval > (end - start) else 0
        while not self.exit and sleepsec > 0:
            sleepsec = sleepsec if sleepsec < max_sleepsec else max_sleepsec
            time.sleep(sleepsec)
            end = time.time()
            sleepsec = self.interval - (end - start) if self.interval > (end - start) else 0


class NonBlockingQueue(Queue):
    dropped = 0

    def nput(self, value):
        """A nonblocking put, that simply logs and discards the value when the
           queue is full, and returns false if we dropped."""
        try:
            self.put(value, False)
        except Full:
            LOG.error("DROPPED LINE: %s", value)
            NonBlockingQueue.dropped += 1
            return False
        return True


# noinspection PyDictCreation
class Sender(threading.Thread):
    def __init__(self, token, readq, options, tags):
        super(Sender, self).__init__()
        self.token = token
        self.exit = False
        self.readq = readq
        self.hosts = options.hosts
        self.http_username = options.http_username
        self.http_password = options.http_password
        self.ssl = options.ssl
        self.tags = tags
        self.maxtags = options.maxtags
        self.dryrun = options.dryrun
        self.current_tsd = -1
        self.blacklisted_hosts = set()
        random.shuffle(self.hosts)

    def shutdown(self):
        LOG.info("signaled sender thread shutdown.")
        self.exit = True

    def run(self):
        """Main loop.  A simple scheduler.  Loop waiting for 5
           seconds for data on the queue.  If there's no data, just
           loop and make sure our connection is still open.  If there
           is data, wait 5 more seconds and grab all of the pending data and
           send it.  A little better than sending every line as its
           own packet."""

        errors = 0  # How many uncaught exceptions in a row we got.
        LOG.info('sender thread started')
        while not self.exit:
            metrics = []
            byte_count = 0
            try:
                try:
                    line = self.readq.get(True, 5)
                except Empty:
                    time.sleep(5)  # Wait for more data
                    continue
                metrics.append(self.process(line))
                byte_count += len(line)
                while byte_count < MAX_SENDQ_SIZE:
                    # prevents self.sendq fast growing in case of sending fails
                    # in send_data()
                    try:
                        line = self.readq.get(False)
                    except Empty:
                        break
                    metrics.append(self.process(line))
                    byte_count += len(line)

                self.send_data_via_http(metrics)
                LOG.info('send %d bytes, readq size %d', byte_count, self.readq.qsize())
                errors = 0  # We managed to do a successful iteration.
            except (ArithmeticError, EOFError, EnvironmentError, LookupError,
                    ValueError):
                errors += 1
                if errors > MAX_UNCAUGHT_EXCEPTIONS:
                    LOG.error("sender thread exceeds the max number of errors (%d). exit", MAX_UNCAUGHT_EXCEPTIONS)
                    shutdown()
                    raise
                LOG.exception('exception in Sender, ignoring')
                time.sleep(1)
                continue
            except:
                LOG.exception('Uncaught exception in Sender, going to exit')
                shutdown()
                raise
        LOG.info('sender thread exited')

    def process(self, line):
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
            subset_metric_keys = frozenset(metric_tags[:len(metric_tags[:self.maxtags - len(metric_entry["tags"])])])
            metric_tags = dict((k, v) for k, v in metric_tags.iteritems() if k in subset_metric_keys)
            LOG.error("Exceeding maximum permitted metric tags - removing %s for metric %s",
                      str(metric_tags_orig - set(metric_tags)), metric)
        metric_entry["tags"].update(metric_tags)
        return metric_entry

    def send_data_via_http(self, metrics):
        data = {'token': self.token, 'metrics': metrics}
        if self.dryrun:
            print "Would have sent:\n%s" % json.dumps(data,
                                                      sort_keys=True,
                                                      indent=4)
            return

        if (self.current_tsd == -1) or (len(self.hosts) > 1):
            self.pick_connection()
        # print "Using server: %s:%s" % (self.host, self.port)
        # url = "http://%s:%s/api/put?details" % (self.host, self.port)
        # print "Url is %s" % url
        LOG.debug("Sending metrics to http://%s:%s/api/put?details",
                  self.host, self.port)
        if self.ssl:
            protocol = "https"
        else:
            protocol = "http"
        req = urllib2.Request("%s://%s:%s/api/put?details" % (
            protocol, self.host, self.port))
        if self.http_username and self.http_password:
            req.add_header("Authorization", "Basic %s"
                           % base64.b64encode("%s:%s" % (self.http_username, self.http_password)))
        req.add_header("Content-Type", "application/json")
        try:
            payload = json.dumps(data)
            LOG.info('put request payload %d', len(payload))
            if self.ssl:
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                response = urllib2.urlopen(req, payload, context=ctx)
            else:
                response = urllib2.urlopen(req, payload)
            LOG.debug("Received response %s", response.getcode())
            # print "Got response code: %s" % response.getcode()
            # print "Content:"
            # for line in response:
            #     print line,
            #     print
        except urllib2.HTTPError, e:
            LOG.exception("Got error when sending to server %s", self.host)
            # for line in http_error:
            #   print line,
        except:
            LOG.exception("unknown error when sending to server %s:%d", self.host, self.port)
            raise

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
        # noinspection PyAttributeOutsideInit
        self.host, self.port = hostport
        LOG.info('Selected connection: %s:%d', self.host, self.port)

    def blacklist_connection(self):
        """Marks the current TSD host we're trying to use as blacklisted.

           Blacklisted hosts will get another chance to be elected once there
           will be no more healthy hosts."""
        LOG.info('Blacklisting %s:%s for a while', self.host, self.port)
        self.blacklisted_hosts.add((self.host, self.port))


if __name__ == '__main__':
    sys.exit(main(sys.argv))
