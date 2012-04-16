#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
This is the Varnish collector module (long-running program) for tcollector,
which can execute on every machine running Varnish, collecting information
as time passes.

Created on Apr 16, 2012

@author: manuel
'''

import sys
import time
import threading
import signal
import logging
import math
import functools

"""
A request from a browser comes in for a Varnish thread to handle
When it comes in (SessionOpen), its state is New-Receiving
It has
- an assigned client thread number

Information about the request comes in
- an URL
- a Host: header
- a Referer: header

Then the request is received completely
Its state is now Received-Processing (VCL_Call recv)

Then one of three things happen

- Varnish triggers an error
- Varnish hits in the cache
- Varnish has to fetch the request

(we skip these three for now, we're going to only handle the error case)

Then the request is processed (TxProtocol)
Its state is now Processed-Replying

Information about the reply comes in
- HTTP status
- Size

Then the request reply is sent fully
Its state is now Replied-Idle
"""

new = "new" # freshly minted
receiving = "receiving" # receiving request from client
retrieving = "retrieving"   # retrieving data from backend
replying = "replying" # replying to client
finished = "finished" # request finished
states = (new,receiving,retrieving,replying,finished)

def percentile(N, percent, key=lambda x:x):
    """
    Find the percentile of a list of values.

    @parameter N - is a list of values. Note N MUST BE already sorted.
    @parameter percent - a float value from 0.0 to 1.0.
    @parameter key - optional key function to compute value from each element of N.

    @return - the percentile of the values
    """
    if not N:
        return None
    k = (len(N)-1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return key(N[int(k)])
    d0 = key(N[int(f)]) * (c-k)
    d1 = key(N[int(c)]) * (k-f)
    return d0+d1

# median is 50th percentile.
median = functools.partial(percentile, percent=0.5)
perc_95th = functools.partial(percentile, percent = 0.95)
perc_99th = functools.partial(percentile, percent = 0.99)

class VarnishRequest:
    state = new
    tid = None
    client_IP = None
    proto = None
    method = None
    URL = None
    host = None
    referrer = None
    user_agent = None
    status = None
    size = None
    backend = None
    cached = "unknown"
    processing_time = None
    lifecycle_time = None
    wow = None
    vcl_seq = None
    lineno = None

    def __init__(self,tid):
        self.tid = tid
        self.wow = []
        self.vcl_seq = []

    def __str__(self):
        ptime = "%.5f"%(self.processing_time * 1000) if self.processing_time is not None else None
        ltime = "%.5f"%(self.lifecycle_time * 1000) if self.lifecycle_time is not None else None
        return """ID:         %s
State:      %s
Method:     %s
URL:        %s
Host:       %s
Referrer:   %s
User agent: %s
From cache: %s
Sequence:   %s
Status:     %s
Proc time:  %s ms
All time:   %s ms
size:       %s bytes
Line #:     %s
WOW:        %s"""%(
                         self.tid,
                         self.state,
                         self.method,
                         self.URL,
                         self.host,
                         self.referrer,
                         self.user_agent,
                         self.cached,
                         self.vcl_seq,
                         self.status,
                         ptime,
                         ltime,
                         self.size,
                         self.lineno,
                         self.wow,
                        )


class ThreadDatabase:
    def __init__(self):
        self.active = {}
        self.finished = []
        self.lock = threading.Lock()
    
    def mark_thread_as_finished(self,thread):
        self.lock.acquire()
        del self.active[thread.tid]
        self.finished.append(thread)
        self.lock.release()
    
    def get(self,tid):
        self.lock.acquire()
        try:
            d = self.active[tid]
        finally:
            self.lock.release()
        return d

    def create(self,tid):
        self.lock.acquire()
        try:
            assert tid not in self.active
            self.active[tid] = VarnishRequest(tid)
            d = self.active[tid]
        finally:
            self.lock.release()
        return d

    def pop_finished(self):
        self.lock.acquire()
        try:
            f,self.finished = self.finished,[]
        finally:
            self.lock.release()
        return f

    def get_pending(self):
        self.lock.acquire()
        try:
            f = dict(self.active)
        finally:
            self.lock.release()
        return f

class Manager:
    def __init__(self,infile=sys.stdin,frequency=1,filter_expression=None):
        self.infile = infile
        self.frequency = frequency
        self.filter_expression = filter_expression
        self.finished = False
        self.logger = logging.getLogger("Manager")
        self.thread_database = ThreadDatabase()
        self.statemachine_thread = threading.Thread(target=self.statemachine)
        self.statemachine_thread.setDaemon(True)
        self.printer_thread = threading.Thread(target=self.print_stats)
        self.printer_thread.setDaemon(True)
        
    def tokenize(self):
        lineno = 0
        while not self.finished:
            lineno = lineno + 1
            line = self.infile.readline()
            if not line.strip(): return
            tid = int(line[0:5].strip())
            func = line[6:19].strip()
            typ = line[19]
            args = line[21:].strip()
            if typ != "c" or tid == 0: continue
            yield (lineno,tid,func,typ,args)
    
    def statemachine(self):
        for lineno,tid,func,typ,args in self.tokenize():
            if func in ("SessionOpen","SessionClose","StatSess"): continue
            if func == "ReqStart":
                thread = self.thread_database.create(tid)
                thread.wow.append( (lineno,tid,func,args) )
                assert thread.state == new, "erroneous state for request in line %s: %s"%(lineno,thread)
                client_IP,_,_ = args.split(" ")
                thread.state = receiving
                thread.client_IP = client_IP
                thread.lineno = lineno
                continue
            
            try:
                thread = self.thread_database.get(tid)
            except KeyError: # This happens when the client sends a session open request but never actually starts the request, closing the connection instead
                continue
    
            thread.wow.append( (lineno,tid,func,args) )
            if func == "RxRequest":
                thread.method = args
            elif func == "RxProtocol":
                thread.protocol = args
            elif func in ("ObjProtocol","ObjStatus","ObjResponse","ObjHeader","TTL","Length","TxResponse","Hit"):
                pass
            elif func == "Backend":
                thread.backend = args
            elif func == "VCL_call":
                thread.vcl_seq.append("call(%s)"%args)
                if args == "hit":
                    thread.cached = "yes"
                elif args in ("miss","pass","fetch"):
                    assert thread.state in (receiving,retrieving)
                    thread.state = retrieving
                    thread.cached = "no"
                elif args == "pipe":
                    assert thread.state in (receiving)
                    thread.state = retrieving
                    thread.cached = "piped"
            elif func == "VCL_return":
                thread.vcl_seq.append("return(%s)"%args)
            elif func == "HitPass":
                thread.cached = "HitPass"
            elif func == "TxProtocol":
                assert thread.state in (receiving,retrieving), "erroneous state for request in line %s: %s"%(lineno,thread)
                thread.state = replying
            elif func == "RxURL":
                thread.URL = args
            elif func == "RxHeader" and args.lower().startswith("host:"):
                try: thread.host = args.split(" ",1)[1]
                except IndexError: self.logger.warn("Malformed Host: header: %s",args)
            elif func == "RxHeader" and args.lower().startswith("referer:"):
                try: thread.referrer = args.split(" ",1)[1]
                except IndexError: self.logger.warn("Malformed Referer: header: %s",args)
            elif func == "RxHeader" and args.lower().startswith("user-agent:"):
                try: thread.user_agent = args.split(" ",1)[1]
                except IndexError: self.logger.warn("Malformed User-Agent: header: %s",args)
            elif func == "RxHeader":
                pass
            elif func == "TxHeader" and args.lower().startswith("content-length:"):
                thread.size = int(args.split(" ",1)[1])
            elif func == "TxHeader":
                pass
            elif func == "LostHeader":
                pass
            elif func == "TxStatus":
                thread.status = int(args)
            elif func == "ReqEnd":
                assert thread.state in (replying,retrieving), "erroneous state for request in line %s: %s"%(lineno,thread)
                thread.state = finished
                start,end = [ float(f) for f in args.split(" ")[1:3] ]
                thread.lifecycle_time = end - start
                thread.processing_time = float(args.split(" ")[4])
                self.thread_database.mark_thread_as_finished(thread)
            else:
                self.logger.exception("not reached, func %r args %r"%(func,args))
        
    def print_stats(self):
        def report():
            stamp = int(time.time())
            t = self.thread_database.pop_finished()
            print "varnish.completed_requests.count %s %s"%(stamp,len(t))
            if t:
                proc_times = sorted([ x.processing_time for x in t ])
                proc_time_median, proc_time_95th, proc_time_99th = median(proc_times), perc_95th(proc_times), perc_99th(proc_times)
                print """varnish.completed_requests.proc_time.median %s %s
varnish.completed_requests.proc_time.95th %s %s
varnish.completed_requests.proc_time.99th %s %s"""%(stamp,proc_time_median,stamp,proc_time_95th,stamp,proc_time_99th)
                life_times = sorted([ x.lifecycle_time for x in t ])
                life_time_median, life_time_95th, life_time_99th = median(life_times), perc_95th(life_times), perc_99th(life_times)
                print """varnish.completed_requests.lifecycle_time.median %s %s
varnish.completed_requests.lifecycle_time.95th %s %s
varnish.completed_requests.lifecycle_time.99th %s %s"""%(stamp,life_time_median,stamp,life_time_95th,stamp,life_time_99th)
            t = self.thread_database.get_pending().values()
            print "varnish.pending_requests.total.count %s %s"%(stamp,len(t))
            for state in states:
                print "varnish.pending_requests.%s.count %s %s"%(state,stamp,len([x for x in t if x.state == state]))
            sys.stdout.flush()
        while not self.finished:
            report()
            time.sleep(self.frequency)
        report()
        return
    
    def run(self):
        self.logger.info("Starting")
        self.statemachine_thread.start()
        self.printer_thread.start()
        self.logger.info("Started")
        while not self.finished:
            time.sleep(1)
        
    def stop(self):
        self.finished = True
        self.infile.close()
    
    def wait(self):
        self.logger.info("Ending")
        self.statemachine_thread.join()
        self.printer_thread.join()
        self.logger.info("Ended")
        
def main():
    logging.basicConfig(level=logging.DEBUG)
    manager = Manager()
    try:
        manager.run()
    except KeyboardInterrupt:
        manager.stop()
    manager.wait()
    return 0

if __name__ == "__main__":
    sys.exit(main())
