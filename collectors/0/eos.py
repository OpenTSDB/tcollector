#!/usr/bin/python
# Copyright (c) 2013, Arista Networks, Inc.
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#
#    * Redistributions in binary form must reproduce the above
#      copyright notice, this list of conditions and the following
#      disclaimer in the documentation and/or other materials provided
#      with the distribution.
#
#    * Neither the name of Arista Networks nor the names of its
#      contributors may be used to endorse or promote products derived
#      from this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL ARISTA
# NETWORKS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
# PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
# OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
# USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
# DAMAGE.


import eossdk
import sys
import time


class IntfCounterCollector(eossdk.TimeoutHandler):

   intf_types = frozenset([eossdk.INTF_TYPE_ETH,
                           eossdk.INTF_TYPE_MANAGEMENT,
                           eossdk.INTF_TYPE_LAG])
   
   def __init__(self, timeout_mgr, intf_mgr, intf_counter_mgr):
      self.intf_mgr_ = intf_mgr
      self.intf_counter_mgr_ = intf_counter_mgr
      self.interval_ = 10
      eossdk.TimeoutHandler.__init__(self, timeout_mgr)
      # Schedule ourselves to run immediately
      self.timeout_time_is(eossdk.now())
   
   def on_timeout(self):
      for intf_id in self.intf_mgr_.intf_iter():
         if intf_id.intf_type() in self.intf_types:
            self.printIntfCounters(intf_id, self.intf_counter_mgr_.counters(intf_id))
      self.timeout_time_is(eossdk.now() + self.interval_)
   
   def printIntfCounters(self, intf_id, intf_counters):
      ts = int(time.time())
      counters = [ 
         ("ucastPkts", "out", intf_counters.out_ucast_pkts()),
         ("multicastPkts", "out", intf_counters.out_multicast_pkts()),
         ("broadcastPkts", "out", intf_counters.out_broadcast_pkts()),
         ("ucastPkts", "in", intf_counters.in_ucast_pkts()),
         ("multicastPkts", "in", intf_counters.in_multicast_pkts()),
         ("broadcastPkts", "in", intf_counters.in_broadcast_pkts()),
         ("octets", "out", intf_counters.out_octets()),
         ("octets", "in", intf_counters.in_octets()),
         ("discards", "out", intf_counters.out_discards()),
         ("errors", "out", intf_counters.out_errors()),
         ("discards", "in", intf_counters.in_discards()),
         ("errors", "in", intf_counters.in_errors()),
         ]
      for counter, direction, value in counters:
         self.printIntfCounter(counter, ts, value, intf_id, direction)
   
   def printIntfCounter(self, counter, ts, value, intf_id, direction):
      print ("eos.interface.%s %d %d iface=%s direction=%s"
             % (counter, ts, value, intf_id.to_string(), direction))


def main():
   sdk = eossdk.Sdk("collector-eos")

   # Create the state managers we're going to poll. For now,
   # we're just pulling information on interface counters
   intf_mgr = sdk.get_intf_mgr()
   intf_counter_mgr = sdk.get_intf_counter_mgr()
   timeout_mgr = sdk.get_timeout_mgr()

   # Kick off the event loop and wait for everything to be initialized
   event_loop = sdk.get_event_loop()
   event_loop.wait_for_initialized()

   # Create a periodic interface counter collector
   intf_counter_collector = IntfCounterCollector(timeout_mgr,
                                                 intf_mgr,
                                                 intf_counter_mgr)
   
   # Run forever
   while True:
      event_loop.run(30)

if __name__ == "__main__":
   sys.exit(main())
