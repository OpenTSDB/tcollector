# Copyright (C) 2014  The tcollector Authors.
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


import imp
import logging
import socket
import sys
import threading
import traceback

import eossdk # pylint: disable=import-error

tracer = eossdk.Tracer("tcollectorAgent")
warn = tracer.trace0
info = tracer.trace1
debug = tracer.trace9

TCOLLECTOR_PATH = "/usr/local/tcollector/tcollector.py"
DEFAULT_TSD_PORT = 4242


class SdkLogger(object):
   """Pretends to be a logging.Logger but logs using EOS SDK.

   Note that this only implements a subset of the logging.Logger API.
   """
   # We do format string expansion in Python to work around BUG116830.

   def __init__(self, name):
      self.tracer = eossdk.Tracer(name)

   def debug(self, msg, *args):
      if self.tracer.enabled(eossdk.Level8):
         self.tracer.trace8("DEBUG: " + msg % args)

   def info(self, msg, *args):
      if self.tracer.enabled(eossdk.Level5):
         self.tracer.trace5("INFO: " + msg % args)

   def warning(self, msg, *args):
      if self.tracer.enabled(eossdk.Level2):
         self.tracer.trace2("WARNING: " + msg % args)

   def error(self, msg, *args):
      if self.tracer.enabled(eossdk.Level1):
         self.tracer.trace1("ERROR: " + msg % args)

   def exception(self, msg, *args):
      if self.tracer.enabled(eossdk.Level1):
         self.tracer.trace1("ERROR: " + msg % args + traceback.format_exc())

   def fatal(self, msg, *args):
      self.tracer.enabled_is(eossdk.Level1, True)
      msg %= args
      self.tracer.trace1(msg)
      assert False, msg

   def setLevel(self, level):
      self.tracer.enabled_is(eossdk.Level8, level <= logging.DEBUG)
      self.tracer.enabled_is(eossdk.Level5, level <= logging.INFO)
      self.tracer.enabled_is(eossdk.Level2, level <= logging.WARNING)
      self.tracer.enabled_is(eossdk.Level1, level <= logging.ERROR)

   @property
   def level(self):
      # TODO: There's currently no API to ask the Tracer what level(s) are enabled.
      # tcollector currently only cares about whether or not debug logging is on.
      if self.tracer.enabled(eossdk.Level8):
         return logging.DEBUG
      elif self.tracer.enabled(eossdk.Level5):
         return logging.INFO
      elif self.tracer.enabled(eossdk.Level2):
         return logging.WARNING
      elif self.tracer.enabled(eossdk.Level1):
         return logging.ERROR
      return logging.CRITICAL

   def addHandler(self, unused_handler):
      pass

   def removeHandler(self, unused_handler):
      pass


class TcollectorAgent(eossdk.AgentHandler,
                      eossdk.SystemHandler,
                      eossdk.TimeoutHandler):

   def __init__(self, sdk):
      eossdk.AgentHandler.__init__(self, sdk.get_agent_mgr())
      eossdk.TimeoutHandler.__init__(self, sdk.get_timeout_mgr())
      eossdk.SystemHandler.__init__(self, sdk.get_system_mgr())
      self.vrf_mgr_ = sdk.get_vrf_mgr()

      # Agent local status
      self.tcollector_running_ = False
      self.shutdown_in_progress_ = False

      self.reader_thread_ = None
      self.sender_thread_ = None
      self.main_thread_ = None
      self.module_ = None
      self.tags_ = None
      debug("TcollectorAgent created")

   def on_initialized(self):
      level = self.get_agent_mgr().agent_option("trace")
      if level:
         self._set_trace(level)
      debug("Agent initialized.")

      # Set up initial status
      self.get_agent_mgr().status_set("has_tcollector_py", "False")

      self.tags_ = { "host": self._get_hostname() }
      # TODO add additional tags

      # Go through the agent startup process.
      self.on_agent_enabled(self.get_agent_mgr().enabled())

   def on_agent_enabled(self, enabled):
      self._maybe_connect()

   def on_agent_option(self, name, value):
      if name == "trace":
         return self._set_trace(value)
      # Options have changed. Attempt to (re)connect.
      self._maybe_connect()

   def _set_trace(self, level):
      level = {
         "debug": logging.DEBUG,
         "info": logging.INFO,
         "warn": logging.WARNING,
         "warning": logging.WARNING,
         "error": logging.ERROR,
      }.get(level.lower())
      if not level:
         level = logging.INFO
      self._import_tcollector()
      self.module_.LOG.setLevel(level)

   def on_timeout(self):
      """ Called when we've tried to shutdown the tcollector process
      and need to wait for it to finish. Since we can't get notified
      asynchronously, this is done out of a timer callback. """
      if self.shutdown_in_progress_:
         # Not yet complete, check again in a second.
         self.timeout_time_is(eossdk.now() + 1)
      else:
         # tcollector shutdown complete. Check to make sure
         # we weren't re-enabled while shutting down.
         self._maybe_connect()

   def _maybe_connect(self):
      self._import_tcollector()

      if self.shutdown_in_progress_:
         debug("tcollector is shutting down, will retry once complete")
         return

      if not self._should_start():
         if self.tcollector_running_:
            # First we have to stop the current tcollector
            self.stop()
         else:
            debug("tcollector already stopped")
            if not self.get_agent_mgr().enabled():
               # Agent has been disabled and tcollector is stopped.
               # Declare cleanup complete
               self.get_agent_mgr().agent_shutdown_complete_is(True)
      else:
         if not self.tcollector_running_:
            self.start()
         else:
            debug("tcollector already running")

   def _should_start(self):
      return (self.module_ is not None
              and self.get_agent_mgr().enabled()
              and self._get_tsd_host())

   def _import_tcollector(self):
      if self.module_ is not None:
         return
      try:
         self.module_ = imp.load_source("tcollector",
                                        TCOLLECTOR_PATH)
         debug("Found tcollector.py")
         self.get_agent_mgr().status_set("has_tcollector_py", "True")
         self.module_.LOG = SdkLogger("tcollector")
         self.module_.setup_logging()
      except IOError as e:
         import errno
         if e.errno != errno.ENOENT:
            raise
         debug("No such file: tcollector.py")

   def _get_hostname(self):
      hostname = self.get_system_mgr().hostname()
      if not hostname or (hostname == "localhost"):
         hostname = socket.gethostname()
      return hostname

   def _get_tsd_host(self):
      return self.get_agent_mgr().agent_option("tsd-host")

   def _get_tsd_port(self):
      tsdPort = self.get_agent_mgr().agent_option("tsd-port")
      if tsdPort and tsdPort.isdigit():
         return int(tsdPort)
      else:
         return DEFAULT_TSD_PORT

   def _socket_at(self, family, socktype, proto):
      vrf = self.get_agent_mgr().agent_option("vrf") or ""
      fd = self.vrf_mgr_.socket_at(family, socktype, proto, vrf)
      return socket._socketobject(_sock=socket.fromfd(fd, family, socktype, proto)) # pylint: disable=no-member

   def on_hostname(self, hostname):
      debug("Hostname changed to", hostname)
      self.tags_["host"] = hostname
      self.sender_thread_.tags = sorted(self.tags_.items())

   def start(self):
      tcollector = self.module_
      tcollector.ALIVE = True
      args = [TCOLLECTOR_PATH,
              "--host", self._get_tsd_host(),
              "--port", str(self._get_tsd_port()),
              "--collector-dir=/usr/local/tcollector/collectors"]

      if self.get_agent_mgr().agent_option("dedup-interval"):
          args.append("--dedup-interval=%s" %
                      self.get_agent_mgr().agent_option("dedup-interval"))

      tcollector.socket.socket = self._socket_at
      debug("Starting tcollector", args)
      options, args = tcollector.parse_cmdline(args)
      tcollector.setup_python_path(TCOLLECTOR_PATH)
      self.tags_["host"] = self._get_hostname()
      modules = tcollector.load_etc_dir(options, self.tags_)

      reader = tcollector.ReaderThread(options.dedupinterval,
                                       options.evictinterval,
                                       options.deduponlyzero)
      self.reader_thread_ = reader
      reader.start()
      debug("ReaderThread startup complete")

      # and setup the sender to start writing out to the tsd
      hosts = [(options.host, options.port)]
      reconnect_interval = 0
      kwargs = {}
      if self.get_agent_mgr().agent_option("transport") == "http":
         kwargs["http"] = True
      elif self.get_agent_mgr().agent_option("transport") == "https":
         kwargs["http"] = True
         kwargs["ssl"] = True
      if self.get_agent_mgr().agent_option("username"):
         kwargs["http_username"] = self.get_agent_mgr().agent_option("username")
      if self.get_agent_mgr().agent_option("password"):
         kwargs["http_password"] = self.get_agent_mgr().agent_option("password")
      sender = tcollector.SenderThread(reader,
                                       options.dryrun,
                                       hosts,
                                       not options.no_tcollector_stats,
                                       self.tags_,
                                       reconnect_interval,
                                       **kwargs)
      self.sender_thread_ = sender
      sender.start()
      debug("SenderThread startup complete")

      self.main_thread_ = threading.Thread(target=self.module_.main_loop,
                                           name="tcollector",
                                           args=(options, modules,
                                                 sender, self.tags_))
      self.main_thread_.start()
      debug("tcollector startup complete")
      self.tcollector_running_ = True

   def stop(self):
      assert not self.shutdown_in_progress_
      self.shutdown_in_progress_ = True

      debug("Telling tcollector to die")
      self.module_.ALIVE = False

      def do_stop():
         debug("Joining main thread")
         self.main_thread_.join()
         debug("Joining ReaderThread thread")
         self.reader_thread_.join()
         debug("Joining SenderThread thread")
         self.sender_thread_.join()
         debug("Killing all remaining collectors")
         for col in list(self.module_.all_living_collectors()):
            col.shutdown()
         # Unregister the collectors...
         self.module_.COLLECTORS.clear()
         debug("Shutdown complete, updating running status")
         self.tcollector_running_ = False
         # Notify that shutdown is complete
         self.shutdown_in_progress_ = False

      # AFAIK we can't join the threads asynchronously, and each thread may
      # take several seconds to join, join the threads with another thread...
      # Kind of a kludge really.
      threading.Thread(target=do_stop, name="stopTcollector").start()

      # Setup timeout handler to poll for stopTcollector thread completion
      self.timeout_time_is(eossdk.now() + 1)

def main():
   sdk = eossdk.Sdk()
   _ = TcollectorAgent(sdk)
   debug("Starting agent")
   sdk.main_loop(sys.argv)
