#!/usr/bin/python
# This file is part of tcollector.
# Copyright (C) 2011  The tcollector Authors.
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

# Note: I spent many hours reading the Linux kernel's source code to infer the
# exact meaning of some of the obscure but useful metrics it exposes.  The
# description of the metrics are correct to the best of my knowledge, but it's
# not always to make sense of the Linux kernel's code.  Please report any
# inaccuracy you find.  -- tsuna.
"""Socket allocation and network statistics for TSDB.

Metrics from /proc/net/sockstat:
  - net.sockstat.num_sockets: Number of sockets allocated (only TCP).
  - net.sockstat.num_timewait: Number of TCP sockets currently in
    TIME_WAIT state.
  - net.sockstat.sockets_inuse: Number of sockets in use (TCP/UDP/raw).
  - net.sockstat.num_orphans: Number of orphan TCP sockets (not attached
    to any file descriptor).
  - net.sockstat.memory: Memory allocated for this socket type (in bytes).
  - net.sockstat.ipfragqueues: Number of IP flows for which there are
    currently fragments queued for reassembly.

Metrics from /proc/net/netstat (`netstat -s' command):
  - net.stat.tcp.abort: Number of connections that the kernel had to abort.
    type=memory is especially bad, the kernel had to drop a connection due to
    having too many orphaned sockets.  Other types are normal (e.g. timeout).
  - net.stat.tcp.abort.failed: Number of times the kernel failed to abort a
    connection because it didn't even have enough memory to reset it (bad).
  - net.stat.tcp.congestion.recovery: Number of times the kernel detected
    spurious retransmits and was able to recover part or all of the CWND.
  - net.stat.tcp.delayedack: Number of delayed ACKs sent of different types.
  - net.stat.tcp.failed_accept: Number of times a connection had to be dropped
    after the 3WHS.  reason=full_acceptq indicates that the application isn't
    accepting connections fast enough.  You should see SYN cookies too.
  - net.stat.tcp.invalid_sack: Number of invalid SACKs we saw of diff types.
    (requires Linux v2.6.24-rc1 or newer)
  - net.stat.tcp.memory.pressure: Number of times a socket entered the
    "memory pressure" mode (not great).
  - net.stat.tcp.memory.prune: Number of times a socket had to discard
    received data due to low memory conditions (bad).
  - net.stat.tcp.packetloss.recovery: Number of times we recovered from packet
    loss by type of recovery (e.g. fast retransmit vs SACK).
  - net.stat.tcp.receive.queue.full: Number of times a received packet had to
    be dropped because the socket's receive queue was full.
    (requires Linux v2.6.34-rc2 or newer)
  - net.stat.tcp.reording: Number of times we detected re-ordering and how.
  - net.stat.tcp.syncookies: SYN cookies (both sent & received).
"""

import re
import resource
import sys
import time

from collectors.lib import utils


def main():
    """Main loop"""
    sys.stdin.close()

    interval = 15
    page_size = resource.getpagesize()

    try:
        sockstat = open("/proc/net/sockstat")
        netstat = open("/proc/net/netstat")
        snmp = open("/proc/net/snmp")
    except IOError, e:
        print >>sys.stderr, "open failed: %s" % e
        return 13  # Ask tcollector to not re-start us.
    utils.drop_privileges()

    # Note: up until v2.6.37-rc2 most of the values were 32 bits.
    # The first value is pretty useless since it accounts for some
    # socket types but not others.  So we don't report it because it's
    # more confusing than anything else and it's not well documented
    # what type of sockets are or aren't included in this count.
    regexp = re.compile("sockets: used \d+\n"
                        "TCP: inuse (?P<tcp_inuse>\d+) orphan (?P<orphans>\d+)"
                        " tw (?P<tw_count>\d+) alloc (?P<tcp_sockets>\d+)"
                        " mem (?P<tcp_pages>\d+)\n"
                        "UDP: inuse (?P<udp_inuse>\d+)"
                        # UDP memory accounting was added in v2.6.25-rc1
                        "(?: mem (?P<udp_pages>\d+))?\n"
                        # UDP-Lite (RFC 3828) was added in v2.6.20-rc2
                        "(?:UDPLITE: inuse (?P<udplite_inuse>\d+)\n)?"
                        "RAW: inuse (?P<raw_inuse>\d+)\n"
                        "FRAG: inuse (?P<ip_frag_nqueues>\d+)"
                        " memory (?P<ip_frag_mem>\d+)\n")

    def print_sockstat(metric, value, tags=""):  # Note: tags must start with ' '
        if value is not None:
            print "net.sockstat.%s %d %s%s" % (metric, ts, value, tags)


    # If a line in /proc/net/{netstat,snmp} doesn't start with a word in that
    # dict, we'll ignore it.  We use the value to build the metric name.
    known_statstypes = {
        "TcpExt:": "tcp",
        "IpExt:": "ip",  # We don't collect anything from here for now.
        "Ip:": "ip",  # We don't collect anything from here for now.
        "Icmp:": "icmp",  # We don't collect anything from here for now.
        "IcmpMsg:": "icmpmsg",  # We don't collect anything from here for now.
        "Tcp:": "tcp",  # We don't collect anything from here for now.
        "Udp:": "udp",
        "UdpLite:": "udplite",  # We don't collect anything from here for now.
        }

    # Any stat in /proc/net/{netstat,snmp} that doesn't appear in this dict will
    # be ignored.  If we find a match, we'll use the (metricname, tags).
    tcp_stats = {
        # An application wasn't able to accept a connection fast enough, so
        # the kernel couldn't store an entry in the queue for this connection.
        # Instead of dropping it, it sent a cookie to the client.
        "SyncookiesSent": ("syncookies", "type=sent"),
        # After sending a cookie, it came back to us and passed the check.
        "SyncookiesRecv": ("syncookies", "type=received"),
        # After sending a cookie, it came back to us but looked invalid.
        "SyncookiesFailed": ("syncookies", "type=failed"),
        # When a socket is using too much memory (rmem), the kernel will first
        # discard any out-of-order packet that has been queued (with SACK).
        "OfoPruned": ("memory.prune", "type=drop_ofo_queue"),
        # If the kernel is really really desperate and cannot give more memory
        # to this socket even after dropping the ofo queue, it will simply
        # discard the packet it received.  This is Really Bad.
        "RcvPruned": ("memory.prune", "type=drop_received"),
        # We waited for another packet to send an ACK, but didn't see any, so
        # a timer ended up sending a delayed ACK.
        "DelayedACKs": ("delayedack", "type=sent"),
        # We wanted to send a delayed ACK but failed because the socket was
        # locked.  So the timer was reset.
        "DelayedACKLocked": ("delayedack", "type=locked"),
        # We sent a delayed and duplicated ACK because the remote peer
        # retransmitted a packet, thinking that it didn't get to us.
        "DelayedACKLost": ("delayedack", "type=lost"),
        # We completed a 3WHS but couldn't put the socket on the accept queue,
        # so we had to discard the connection.
        "ListenOverflows": ("failed_accept", "reason=full_acceptq"),
        # We couldn't accept a connection because one of: we had no route to
        # the destination, we failed to allocate a socket, we failed to
        # allocate a new local port bind bucket.  Note: this counter
        # also include all the increments made to ListenOverflows...
        "ListenDrops": ("failed_accept", "reason=other"),
        # A packet was lost and we used Forward RTO-Recovery to retransmit.
        "TCPForwardRetrans": ("retransmit", "type=forward"),
        # A packet was lost and we fast-retransmitted it.
        "TCPFastRetrans": ("retransmit", "type=fast"),
        # A packet was lost and we retransmitted after a slow start.
        "TCPSlowStartRetrans": ("retransmit", "type=slowstart"),
        # A packet was lost and we recovered after a fast retransmit.
        "TCPRenoRecovery": ("packetloss.recovery", "type=fast_retransmit"),
        # A packet was lost and we recovered by using selective
        # acknowledgements.
        "TCPSackRecovery": ("packetloss.recovery", "type=sack"),
        # We detected re-ordering using FACK (Forward ACK -- the highest
        # sequence number known to have been received by the peer when using
        # SACK -- FACK is used during congestion control).
        "TCPFACKReorder": ("reording", "detectedby=fack"),
        # We detected re-ordering using SACK.
        "TCPSACKReorder": ("reording", "detectedby=sack"),
        # We detected re-ordering using fast retransmit.
        "TCPRenoReorder": ("reording", "detectedby=fast_retransmit"),
        # We detected re-ordering using the timestamp option.
        "TCPTSReorder": ("reording", "detectedby=timestamp"),
        # We detected some erroneous retransmits and undid our CWND reduction.
        "TCPFullUndo": ("congestion.recovery", "type=full_undo"),
        # We detected some erroneous retransmits, a partial ACK arrived while
        # we were fast retransmitting, so we were able to partially undo some
        # of our CWND reduction.
        "TCPPartialUndo": ("congestion.recovery", "type=hoe_heuristic"),
        # We detected some erroneous retransmits, a D-SACK arrived and ACK'ed
        # all the retransmitted data, so we undid our CWND reduction.
        "TCPDSACKUndo": ("congestion.recovery", "type=sack"),
        # We detected some erroneous retransmits, a partial ACK arrived, so we
        # undid our CWND reduction.
        "TCPLossUndo": ("congestion.recovery", "type=ack"),
        # We received an unexpected SYN so we sent a RST to the peer.
        "TCPAbortOnSyn": ("abort", "type=unexpected_syn"),
        # We were in FIN_WAIT1 yet we received a data packet with a sequence
        # number that's beyond the last one for this connection, so we RST'ed.
        "TCPAbortOnData": ("abort", "type=data_after_fin_wait1"),
        # We received data but the user has closed the socket, so we have no
        # wait of handing it to them, so we RST'ed.
        "TCPAbortOnClose": ("abort", "type=data_after_close"),
        # This is Really Bad.  It happens when there are too many orphaned
        # sockets (not attached a FD) and the kernel has to drop a connection.
        # Sometimes it will send a reset to the peer, sometimes it wont.
        "TCPAbortOnMemory": ("abort", "type=out_of_memory"),
        # The connection timed out really hard.
        "TCPAbortOnTimeout": ("abort", "type=timeout"),
        # We killed a socket that was closed by the application and lingered
        # around for long enough.
        "TCPAbortOnLinger": ("abort", "type=linger"),
        # We tried to send a reset, probably during one of teh TCPABort*
        # situations above, but we failed e.g. because we couldn't allocate
        # enough memory (very bad).
        "TCPAbortFailed": ("abort.failed", None),
        # Number of times a socket was put in "memory pressure" due to a non
        # fatal memory allocation failure (reduces the send buffer size etc).
        "TCPMemoryPressures": ("memory.pressure", None),
        # We got a completely invalid SACK block and discarded it.
        "TCPSACKDiscard": ("invalid_sack", "type=invalid"),
        # We got a duplicate SACK while retransmitting so we discarded it.
        "TCPDSACKIgnoredOld": ("invalid_sack", "type=retransmit"),
        # We got a duplicate SACK and discarded it.
        "TCPDSACKIgnoredNoUndo": ("invalid_sack", "type=olddup"),
        # We received something but had to drop it because the socket's
        # receive queue was full.
        "TCPBacklogDrop": ("receive.queue.full", None),
    }
    known_stats = {
        "tcp": tcp_stats,
        "ip": {
        },
        "icmp": {
        },
        "icmpmsg": {
        },
        "udp": {
            # Total UDP datagrams received by this host
            "InDatagrams": ("datagrams", "direction=in"),
            # UDP datagrams received on a port with no listener
            "NoPorts": ("errors", "direction=in reason=noport"),
            # Total UDP datagrams that could not be delivered to an application
            # Note: this counter also increments for RcvbufErrors
            "InErrors": ("errors", "direction=in reason=other"),
            # Total UDP datagrams sent from this host
            "OutDatagrams": ("datagrams", "direction=out"),
            # Datagrams for which not enough socket buffer memory to receive
            "RcvbufErrors": ("errors", "direction=in reason=nomem"),
            # Datagrams for which not enough socket buffer memory to transmit
            "SndbufErrors": ("errors", "direction=out reason=nomem"),
        },
        "udplite": {
        },
    }


    def print_netstat(statstype, metric, value, tags=""):
        if tags:
            space = " "
        else:
            tags = space = ""
        print "net.stat.%s.%s %d %s%s%s" % (statstype, metric, ts, value,
                                            space, tags)

    def parse_stats(stats, filename):
        statsdikt = {}
        # /proc/net/{netstat,snmp} have a retarded column-oriented format.  It
        # looks like this:
        #   Header: SomeMetric OtherMetric
        #   Header: 1 2
        #   OtherHeader: ThirdMetric FooBar
        #   OtherHeader: 42 51
        #   OtherHeader: FourthMetric
        #   OtherHeader: 4
        # We first pair the lines together, then create a dict for each type:
        #   {"SomeMetric": "1", "OtherMetric": "2"}
        lines = stats.splitlines()
        assert len(lines) % 2 == 0, repr(lines)
        for header, data in zip(*(iter(lines),) * 2):
            header = header.split()
            data = data.split()
            assert header[0] == data[0], repr((header, data))
            assert len(header) == len(data), repr((header, data))
            if header[0] not in known_statstypes:
                print >>sys.stderr, ("Unrecoginized line in %s:"
                                     " %r (file=%r)" % (filename, header, stats))
                continue
            statstype = header.pop(0)
            data.pop(0)
            stats = dict(zip(header, data))
            statsdikt.setdefault(known_statstypes[statstype], {}).update(stats)
        for statstype, stats in statsdikt.iteritems():
            # Undo the kernel's double counting
            if "ListenDrops" in stats:
                stats["ListenDrops"] = int(stats["ListenDrops"]) - int(stats.get("ListenOverflows", 0))
            elif "RcvbufErrors" in stats:
                stats["InErrors"] = int(stats.get("InErrors", 0)) - int(stats["RcvbufErrors"])
            for stat, (metric, tags) in known_stats[statstype].iteritems():
                value = stats.get(stat)
                if value is not None:
                    print_netstat(statstype, metric, value, tags)

    while True:
        ts = int(time.time())
        sockstat.seek(0)
        netstat.seek(0)
        snmp.seek(0)
        data = sockstat.read()
        netstats = netstat.read()
        snmpstats = snmp.read()
        m = re.match(regexp, data)
        if not m:
            print >>sys.stderr, "Cannot parse sockstat: %r" % data
            return 13

        # The difference between the first two values is the number of
        # sockets allocated vs the number of sockets actually in use.
        print_sockstat("num_sockets",   m.group("tcp_sockets"),   " type=tcp")
        print_sockstat("num_timewait",  m.group("tw_count"))
        print_sockstat("sockets_inuse", m.group("tcp_inuse"),     " type=tcp")
        print_sockstat("sockets_inuse", m.group("udp_inuse"),     " type=udp")
        print_sockstat("sockets_inuse", m.group("udplite_inuse"), " type=udplite")
        print_sockstat("sockets_inuse", m.group("raw_inuse"),     " type=raw")

        print_sockstat("num_orphans", m.group("orphans"))
        print_sockstat("memory", int(m.group("tcp_pages")) * page_size,
                       " type=tcp")
        if m.group("udp_pages") is not None:
          print_sockstat("memory", int(m.group("udp_pages")) * page_size,
                         " type=udp")
        print_sockstat("memory", m.group("ip_frag_mem"), " type=ipfrag")
        print_sockstat("ipfragqueues", m.group("ip_frag_nqueues"))

        parse_stats(netstats, netstat.name)
        parse_stats(snmpstats, snmp.name)

        sys.stdout.flush()
        time.sleep(interval)

if __name__ == "__main__":
    sys.exit(main())
