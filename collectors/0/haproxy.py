#!/usr/bin/python
# HaProxy stat collector.
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
"""haproxy stats for TSDB """

#
# haproxy.py
#

import socket
import sys
import time
import urllib
import csv
import re

COLLECTION_INTERVAL = 10  # seconds

BACKEND_SERVICES = { 'cluster_mysql_read' : 'mysql', 'static' : 'static', 'dynamic' : 'dynamic', 'varnish' : 'varnish' }
FRONTEND_SERVICES = { 'images' : 'images', 'app' : 'app', 'mysql_read' : 'mysql', 'intern_dynamic' : 'varnish' }

#HAPROXY_CSV_STAT_URL = 'file:///Users/fmk/Desktop/stats.csv'
HAPROXY_CSV_STAT_URL = 'http://admin:GF123NET@62.146.7.79/admin/stats?;csv'

def main():
    """haproxy main loop"""
    csvfile = None
    
    while True:
        ts = int(time.time())
        
        try:
            csvfile = urllib.urlopen(HAPROXY_CSV_STAT_URL);
            
            reader = csv.reader(csvfile, delimiter=',', quoting=csv.QUOTE_NONE)
            for row in reader:

                
                """ collect all the frontend aggregates """
                if row[0] in FRONTEND_SERVICES and row[1] == 'FRONTEND':
                    
                    """ only collect http info for http services """
                    if FRONTEND_SERVICES[row[0]] is not 'mysql':
                        
                        print ("haproxy.requests.total %d %s service=%s interface=frontend" % (ts, row[48], FRONTEND_SERVICES[row[0]]))
                    
                        print ("haproxy.requests %d %s code=5xx service=%s" % (ts, row[43], FRONTEND_SERVICES[row[0]]))
                        print ("haproxy.requests %d %s code=4xx service=%s" % (ts, row[42], FRONTEND_SERVICES[row[0]]))
                        print ("haproxy.requests %d %s code=3xx service=%s" % (ts, row[41], FRONTEND_SERVICES[row[0]]))
                        print ("haproxy.requests %d %s code=2xx service=%s" % (ts, row[40], FRONTEND_SERVICES[row[0]]))
                        print ("haproxy.requests %d %s code=1xx service=%s" % (ts, row[39], FRONTEND_SERVICES[row[0]]))
                    
                    
                    print ("haproxy.session.total %d %s service=%s" % (ts, row[7], FRONTEND_SERVICES[row[0]]))
                    print ("haproxy.session.current %d %s service=%s" % (ts, row[4], FRONTEND_SERVICES[row[0]]))
                    
                    print ("haproxy.session.max %d %s service=%s" % (ts, row[5], FRONTEND_SERVICES[row[0]]))
               
                
                """ collect global backend statistics """
                if row[0] in BACKEND_SERVICES and row[1] == 'BACKEND':
                    
                    """ only collect http info for http services """
                    if BACKEND_SERVICES[row[0]] is not 'mysql':
                        
                        print ("haproxy.backend.requests.total %d %s service=%s" % (ts, row[48], BACKEND_SERVICES[row[0]]))
                        
                        print ("haproxy.backend.requests %d %s code=5xx service=%s" % (ts, row[43], BACKEND_SERVICES[row[0]]))
                        print ("haproxy.backend.requests %d %s code=4xx service=%s" % (ts, row[42], BACKEND_SERVICES[row[0]]))
                        print ("haproxy.backend.requests %d %s code=3xx service=%s" % (ts, row[41], BACKEND_SERVICES[row[0]]))
                        print ("haproxy.backend.requests %d %s code=2xx service=%s" % (ts, row[40], BACKEND_SERVICES[row[0]]))
                        print ("haproxy.backend.requests %d %s code=1xx service=%s" % (ts, row[39], BACKEND_SERVICES[row[0]]))
                    
                    
                    print ("haproxy.backend.session.total %d %s service=%s" % (ts, row[7], BACKEND_SERVICES[row[0]]))
                    print ("haproxy.backend.session.current %d %s service=%s" % (ts, row[4], BACKEND_SERVICES[row[0]]))
                    
                    print ("haproxy.backend.session.max %d %s service=%s" % (ts, row[5], BACKEND_SERVICES[row[0]]))

                elif row[0] in BACKEND_SERVICES and row[1] != 'BACKEND':
                    
                    """ only collect http info for http services """
                    if BACKEND_SERVICES[row[0]] is not 'mysql':
                        
                        print ("haproxy.node.requests.total %d %s service=%s node=%s" % (ts, row[48], BACKEND_SERVICES[row[0]], row[1]))
                        
                        print ("haproxy.node.requests %d %s code=5xx service=%s node=%s" % (ts, row[43], BACKEND_SERVICES[row[0]], row[1]))
                        print ("haproxy.node.requests %d %s code=4xx service=%s node=%s" % (ts, row[42], BACKEND_SERVICES[row[0]], row[1]))
                        print ("haproxy.node.requests %d %s code=3xx service=%s node=%s" % (ts, row[41], BACKEND_SERVICES[row[0]], row[1]))
                        print ("haproxy.node.requests %d %s code=2xx service=%s node=%s" % (ts, row[40], BACKEND_SERVICES[row[0]], row[1]))
                        print ("haproxy.node.requests %d %s code=1xx service=%s node=%s" % (ts, row[39], BACKEND_SERVICES[row[0]], row[1]))
                    
                    
                    print ("haproxy.node.session.total %d %s service=%s node=%s" % (ts, row[7], BACKEND_SERVICES[row[0]], row[1]))
                    print ("haproxy.node.session.current %d %s service=%s node=%s" % (ts, row[4], BACKEND_SERVICES[row[0]], row[1]))
                    
                    print ("haproxy.node.session.max %d %s service=%s node=%s" % (ts, row[5], BACKEND_SERVICES[row[0]], row[1]))
    
                    print ("haproxy.node.downtime %d %s service=%s node=%s" % (ts, row[24], BACKEND_SERVICES[row[0]], row[1]))
                                   
            csvfile.close()
                 
        except socket.error, (errno, msg):
            print >> sys.stderr, "haproxy returned %r" % msg
            if csvfile is not None:
                csvfile.close()
                csvfile = None;
                
        sys.stdout.flush()
        time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
    main()
