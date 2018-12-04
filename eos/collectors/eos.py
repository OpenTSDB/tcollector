#!/usr/bin/env python
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


try:
   import jsonrpclib
except ImportError:
   jsonrpclib = None

import os
import sys
import time


class IntfCounterCollector(object):

   def __init__(self, server):
      self.server_ = server

   def printIntfCounters(self):
      ts = int(time.time())

      result = self.server_.runCmds(1, ["show interfaces counters",
                                        "show interfaces counters errors",
                                        "show interfaces counters bins"])
      jsonrpclib.history.clear()
      (counters, error_counters, bin_counters) = result

      # Print general interface counters
      counter_definitions = [
         ("ucastPkts", {"direction" : "out"}, "outUcastPkts"),
         ("multicastPkts", {"direction" : "out"}, "outMulticastPkts"),
         ("broadcastPkts", {"direction" : "out"}, "outBroadcastPkts"),
         ("ucastPkts", {"direction" : "in"}, "inUcastPkts"),
         ("multicastPkts", {"direction" : "in"}, "inMulticastPkts"),
         ("broadcastPkts", {"direction" : "in"}, "inBroadcastPkts"),
         ("octets", {"direction" : "out"}, "outOctets"),
         ("octets", {"direction" : "in"}, "inOctets"),
         ("discards", {"direction" : "out"}, "outDiscards"),
         ("discards", {"direction" : "in"}, "inDiscards"),
      ]
      for intf_id, intf_counters in counters["interfaces"].items():
         for counter_name, tags, eos_counter_name in counter_definitions:
            if eos_counter_name in intf_counters:
               self.printIntfCounter(counter_name, ts, intf_counters[eos_counter_name],
                                     intf_id, tags)

      # Print interface error counters
      error_counter_definitions = [
         ("errors", {"direction" : "out"}, "outErrors"),
         ("errors", {"direction" : "in"}, "inErrors"),
         ("frameTooShorts", {}, "frameTooShorts"),
         ("fcsErrors", {}, "fcsErrors"),
         ("alignmentErrors", {}, "alignmentErrors"),
         ("symbolErrors", {}, "symbolErrors"),
      ]
      for intf_id, intf_error_counters in error_counters["interfaceErrorCounters"].items():
         for counter_name, tags, eos_counter_name in error_counter_definitions:
            if eos_counter_name in intf_error_counters:
               self.printIntfCounter(counter_name, ts, intf_error_counters[eos_counter_name],
                                     intf_id, tags)

      # Print interface bin counters
      for intf_id, intf_bin_counters in bin_counters["interfaces"].items():
         for direction in ["in", "out"]:
            if not intf_bin_counters.get("%sBinsCounters" % direction):
               continue
            for binSize in ["64", "65To127", "128To255", "256To511",
                        "512To1023", "1024To1522", "1523ToMax"]:
               value = intf_bin_counters["%sBinsCounters" % direction]["frames%sOctet" % binSize]
               tags = {"size" : binSize, "direction" : direction}
            self.printIntfCounter("frameBySize", ts, value, intf_id, tags)

      sys.stdout.flush()

   def printIntfCounter(self, counter, ts, value, intf_id, tags):
      tag_str = " ".join(["%s=%s" % (tag_name, tag_value) for
                          (tag_name, tag_value) in tags.items()])
      sys.stdout.write("eos.interface.%s %d %d iface=%s %s\n"
             % (counter, ts, value, intf_id, tag_str))


def main():
   commandApiSocket = "/var/run/command-api.sock"
   if (jsonrpclib == None) or not os.path.exists(commandApiSocket):
      return 13 # Ask tcollector to not respawn us

   server = jsonrpclib.Server("unix:%s" % commandApiSocket)
   interval = 30

   intfCounterCollector = IntfCounterCollector(server)

   while True:
      intfCounterCollector.printIntfCounters()
      time.sleep(interval)


if __name__ == "__main__":
   sys.exit(main())
