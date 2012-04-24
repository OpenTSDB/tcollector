#!/usr/bin/python
# Memcached stat collector.
# Copyright (C) 2011  Gutefrage.net GmbH.
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
"""memcached stats for TSDB """

"""
memcached opentsdb keys

memcached.auth_cmds
memcached.reclaimed
memcached.cas_hits
memcached.uptime
memcached.delete_misses
memcached.listen_disabled_num
memcached.cas_misses
memcached.decr_hits
memcached.incr_hits
memcached.limit_maxbytes
memcached.bytes_written
memcached.incr_misses
memcached.accepting_conns
memcached.rusage_system
memcached.total_items
memcached.cmd_get
memcached.curr_connections
memcached.total_connections
memcached.cmd_set
memcached.curr_items
memcached.conn_yields
memcached.get_misses
memcached.bytes_read
memcached.cas_badval
memcached.cmd_flush
memcached.evictions
memcached.bytes
memcached.connection_structures
memcached.auth_errors
memcached.rusage_user
memcached.delete_hits
memcached.decr_misses
memcached.get_hits

"""
#
# memcached.py
#

import socket
import sys
import time
import re
import telnetlib


COLLECTION_INTERVAL = 3  # seconds

MEMCACHED_STAT_CMD = 'stats'
MEMCACHED_STAT_REGEX = re.compile(ur"STAT (.*) (.*)\r")
MEMCACHED_STAT_OMIT = ['version', 'threads', 'pid', 'time', 'pointer_size']

def main():
    """memcached main loop"""
    client = None
    
    while True:
        ts = int(time.time())
        
        try:
            
            if client is None:
                client = telnetlib.Telnet('localhost', '11211')
            
            client.write("%s\n" % MEMCACHED_STAT_CMD)
            response = dict(MEMCACHED_STAT_REGEX.findall(client.read_until('END')))
    
            for stat,val in response.iteritems():
                if stat not in MEMCACHED_STAT_OMIT:
                    print ("memcached.%s %d %s" % (stat, ts, val))
        
        except socket.error, (errno, msg):
            print >> sys.stderr, "memcached returned %r" % msg
            if client is not None:
                client.close()
                client = None;
        except EOFError:
            print >> sys.stderr, "memcached connection was closed before reading stats"
            if client is not None:
                client.close()
                client = None;
                
        sys.stdout.flush()       
        time.sleep(COLLECTION_INTERVAL)

    client.close()

if __name__ == "__main__":
    main()
