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


try:
   import eossdk
except ImportError:
   eossdk = None
   
import sys
import time

# pylint: disable-msg=E1101

class IntfCounterCollector(eossdk.AgentHandler,
                           eossdk.TimeoutHandler):

   intf_types = frozenset([eossdk.INTF_TYPE_ETH,
                           eossdk.INTF_TYPE_MANAGEMENT,
                           eossdk.INTF_TYPE_LAG])
   
   def __init__(self, agent_mgr, timeout_mgr, intf_mgr,
                intf_counter_mgr, eth_intf_counter_mgr):
      self.intf_mgr_ = intf_mgr
      self.intf_counter_mgr_ = intf_counter_mgr
      self.eth_intf_counter_mgr_ = eth_intf_counter_mgr
      self.interval_ = 30
      # pylint: disable-msg=W0233
      eossdk.AgentHandler.__init__(self, agent_mgr)
      eossdk.TimeoutHandler.__init__(self, timeout_mgr)
   
   def on_initialized(self):
      # Schedule ourselves to run immediately
      self.timeout_time_is(eossdk.now())

   def on_timeout(self):
      for intf_id in self.intf_mgr_.intf_iter():
         if intf_id.intf_type() in self.intf_types:
            self.printIntfCounters(intf_id)
      self.timeout_time_is(eossdk.now() + self.interval_)
   
   def printIntfCounters(self, intf_id):
      ts = int(time.time())
      
      self.intf_counter_mgr_.counters(intf_id)
      intf_counters = self.intf_counter_mgr_.counters(intf_id)
      counters = [ 
         ("ucastPkts", {"direction" : "out"},
          intf_counters.out_ucast_pkts()),
         ("multicastPkts", {"direction" : "out"},
          intf_counters.out_multicast_pkts()),
         ("broadcastPkts", {"direction" : "out"},
          intf_counters.out_broadcast_pkts()),
         ("ucastPkts", {"direction" : "in"},
          intf_counters.in_ucast_pkts()),
         ("multicastPkts", {"direction" : "in"},
          intf_counters.in_multicast_pkts()),
         ("broadcastPkts", {"direction" : "in"},
          intf_counters.in_broadcast_pkts()),
         ("octets", {"direction" : "out"},
          intf_counters.out_octets()),
         ("octets", {"direction" : "in"},
          intf_counters.in_octets()),
         ("discards", {"direction" : "out"},
          intf_counters.out_discards()),
         ("errors", {"direction" : "out"},
          intf_counters.out_errors()),
         ("discards", {"direction" : "in"},
          intf_counters.in_discards()),
         ("errors", {"direction" : "in"},
          intf_counters.in_errors()),
         ]
      for counter, tags, value in counters:
         self.printIntfCounter(counter, ts, value, intf_id, tags)

      if intf_id.intf_type() == eossdk.INTF_TYPE_ETH:
         eth_intf_counters = self.eth_intf_counter_mgr_.counters(intf_id)
         eth_counters = [
            ("singleCollisionFrames", {},
             eth_intf_counters.single_collision_frames()),
            ("multipleCollisionFrames", {},
             eth_intf_counters.multiple_collision_frames()),
            ("fcsErrors", {},
             eth_intf_counters.fcs_errors()),
            ("alignmentErrors", {},
             eth_intf_counters.alignment_errors()),
            ("deferredTransmissions", {},
             eth_intf_counters.deferred_transmissions()),
            ("lateCollisions", {},
             eth_intf_counters.late_collisions()),
            ("excessiveCollisions", {},
             eth_intf_counters.excessive_collisions()),
            ("internalMacTransmitErrors", {},
             eth_intf_counters.internal_mac_transmit_errors()),
            ("carrierSenseErrors", {},
             eth_intf_counters.carrier_sense_errors()),
            ("internalMacReceiveErrors", {},
             eth_intf_counters.internal_mac_receive_errors()),
            ("frameTooShorts", {},
             eth_intf_counters.frame_too_shorts()),
            ("sqe_testErrors", {},
             eth_intf_counters.sqe_test_errors()),
            ("symbolErrors", {},
             eth_intf_counters.symbol_errors()),
            ("unknownOpcodes", {"direction" : "in"},
             eth_intf_counters.in_unknown_opcodes()),
            ("pauseFrames", {"direction" : "out"},
             eth_intf_counters.out_pause_frames()),
            ("pauseFrames", {"direction" : "in"},
             eth_intf_counters.in_pause_frames()),
            ("fragments", {},
             eth_intf_counters.fragments()),
            ("jabbers", {},
             eth_intf_counters.jabbers()),
            ]
         for counter, tags, value in eth_counters:
            self.printIntfCounter(counter, ts, value, intf_id, tags)
         
         eth_intf_bin_counters = self.eth_intf_counter_mgr_.bin_counters(intf_id)
         eth_bin_counters = [
            ("frameBySize", {"size" : "64", "direction" : "in"},
             eth_intf_bin_counters.in_64_octet_frames()),
            ("frameBySize", {"size" : "65To127", "direction" : "in"},
             eth_intf_bin_counters.in_65_to_127_octet_frames()),
            ("frameBySize", {"size" : "128To255", "direction" : "in"},
             eth_intf_bin_counters.in_128_to_255_octet_frames()),
            ("frameBySize", {"size" : "256To511", "direction" : "in"},
             eth_intf_bin_counters.in_256_to_511_octet_frames()),
            ("frameBySize", {"size" : "512To1023", "direction" : "in"},
             eth_intf_bin_counters.in_512_to_1023_octet_frames()),
            ("frameBySize", {"size" : "1024To1522", "direction" : "in"},
             eth_intf_bin_counters.in_1024_to_1522_octet_frames()),
            ("frameBySize", {"size" : "1522ToMax", "direction" : "in"},
             eth_intf_bin_counters.in_1522_to_max_octet_frames()),
            ("frameBySize", {"size" : "64", "direction" : "out"},
             eth_intf_bin_counters.out_64_octet_frames()),
            ("frameBySize", {"size" : "65To127", "direction" : "out"},
             eth_intf_bin_counters.out_65_to_127_octet_frames()),
            ("frameBySize", {"size" : "128To255", "direction" : "out"},
             eth_intf_bin_counters.out_128_to_255_octet_frames()),
            ("frameBySize", {"size" : "256To511", "direction" : "out"},
             eth_intf_bin_counters.out_256_to_511_octet_frames()),
            ("frameBySize", {"size" : "512To1023", "direction" : "out"},
             eth_intf_bin_counters.out_512_to_1023_octet_frames()),
            ("frameBySize", {"size" : "1024To1522", "direction" : "out"},
             eth_intf_bin_counters.out_1024_to_1522_octet_frames()),
            ("frameBySize", {"size" : "1522ToMax", "direction" : "out"},
             eth_intf_bin_counters.out_1522_to_max_octet_frames()),
            ]
         for counter, tags, value in eth_bin_counters:
            self.printIntfCounter(counter, ts, value, intf_id, tags)
      
   def printIntfCounter(self, counter, ts, value, intf_id, tags):
      tag_str = " ".join(["%s=%s" % (tag, value) for (tag, value) in tags.items()])
      print ("eos.interface.%s %d %d iface=%s %s"
             % (counter, ts, value, intf_id.to_string(), tag_str))


def main():
   if eossdk == None:
      # This collector requires the eossdk module
      return 13 # Ask tcollector to not respawn us
   
   sdk = eossdk.Sdk("tcollector-eos")

   # Create the state managers we're going to poll. For now,
   # we're just pulling information on interface counters
   intf_mgr = sdk.get_intf_mgr()
   intf_counter_mgr = sdk.get_intf_counter_mgr()
   timeout_mgr = sdk.get_timeout_mgr()

   # Create a periodic interface counter collector
   _ = IntfCounterCollector(timeout_mgr, intf_mgr, intf_counter_mgr)
   
   # Start the main loop
   sdk.main_loop(sys.argv)


if __name__ == "__main__":
   sys.exit(main())
