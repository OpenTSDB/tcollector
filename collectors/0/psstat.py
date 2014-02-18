#!/usr/bin/env python
"""
 v 0.1-4
 This requires a configuration file to be placed a /<class>/shared/conf/psstat.conf
 The format of the configuration is:
 imagename<white space>regex
 regex is matched against the processes' full commandline
 image is the tag you can use in TSD as image=imagename

The configuration will be automatically reloaded when the config file is updated.
Comments are support and are specified by a line where the first character is #

All of the information presented is gathered from /proc
Where there are multiple pids matching the specified image, the values are summed

If you get unexected output, set: DEBUG = True
"""

import os
from socket import gethostname
from sys import stderr, exit, stdout
from time import sleep, time
from re import match, search, compile, sub
from threading import Thread
from Queue import Queue

DEBUG = False
COLLECTION_INTERVAL = 15
#Configuration will be read from /<class>/shared/conf/psstat.conf
CONF_SUFFIX = "/shared/conf/psstat.conf"
METRIC_PREFIX = "proc.stat.ps"
#Maximum length of input lines
MAX_LINE_LEN = 2048
CLOCKTICK = os.sysconf('SC_CLK_TCK')
THREADS = 15
#Time to wait in a loop for config file to be populated
CONF_WAIT = 180
#To speficy a custom path and file for configuration, place it here. eg. CONF_OVERRIDE = "/my/path/to/myconf.conf"
#Otherwise leave this as key word None for normal operation
CONF_OVERRIDE = None


class Worker(Thread):
    def __init__(self, inq, outq, func):
        Thread.__init__(self)
        self.inq = inq
        self.outq = outq
        self.func = func
        self.start()

    def run(self):
        while not self.inq.empty():
            try:
                val = self.inq.get(False)
            except Exception, err:
                if DEBUG: stderr.write(str(err))
                continue
            out = self.func(val[0], val[1])
            if out is None: continue
            self.outq.put([val[0], out])


class ConnTable(object):
    def __init__(self):
        self.tcpinodes = dict()
        self.tcp6inodes = dict()
        self.udpinodes = dict()
        self.udp6inodes = dict()
        self.unixinodes = dict()


def getconf1():
    if not CONF_OVERRIDE:
        try:
            hostname = gethostname()
            if DEBUG: stderr.write("Hostname is " + hostname + "\n")
        except Exception, err:
            stderr.write("Unable to get hostname: %s\n" % str(err))
            exit(1)
        hostname = hostname.split('.')
        hostname = hostname[0].rstrip("0,1,2,3,4,5,6,7,8,9")
        confpath = "/" + hostname + CONF_SUFFIX
    else:
        confpath = CONF_OVERRIDE
    try:
        cf = open(confpath, "r")
    except Exception, err:
        stderr.write("Failed to open config file " + confpath + ": %s\n" % str(err))
        exit(1)
    oneline = cf.readline(MAX_LINE_LEN)
    mtime = os.stat(confpath).st_mtime
    fconf = []
    linenum = 1
    while oneline:
        if match("^#", oneline):
            oneline = cf.readline(MAX_LINE_LEN)
            linenum += 1
            continue
        oneline = oneline.rstrip()
        oneline = oneline.split()
        if len(oneline) != 2:
            if len(oneline) > 0:
                stderr.write("Skipping bad config at line %d : %s\n" % (linenum, oneline))
            oneline = cf.readline(MAX_LINE_LEN)
            linenum += 1
            continue
        try:
            oneline[1] = compile(oneline[1])
        except Exception, err:
            stderr.write("Skipping regex at line %d : %s\n" % (linenum, err))
            oneline = cf.readline(MAX_LINE_LEN)
            linenum += 1
            continue
        fconf.append(oneline)
        oneline = cf.readline(MAX_LINE_LEN)
        linenum += 1
    if len(fconf) < 1:
        return (None, None, confpath)
    return (fconf, mtime, confpath)


def getpids(proclist):
    allpids = [f for f in os.listdir('/proc') if match(r'[0-9]+.', f)]
    wantedpids = {}
    for p in allpids:
        pth = "/proc/%s/cmdline" % p
        try:
            pf = open(pth, "r")
        except Exception, err:
            if DEBUG: stderr.write("Failed to open %s: %s\n" % (pth, str(err)))
            continue
        cline = pf.readline(MAX_LINE_LEN)
        cline = sub('\x00', ' ', cline)
        for ps in proclist:
            if search(ps[1], cline):
                if ps[0] not in wantedpids:
                    wantedpids[ps[0]] = []
                wantedpids[ps[0]].append(p)
    return wantedpids


def getio(proc, pidlist):
    data = {}
    for pid in pidlist:
        iopath = "/proc" + "/" + pid + "/" + "io"
        try:
            iof = open(iopath, "r")
        except Exception, err:
            if DEBUG: stderr.write(iopath + " :%s\n" % str(err))
            continue
        oneline = iof.readline(MAX_LINE_LEN)
        oneline = oneline.rstrip()
        while oneline:
            l = oneline.split(':')
            if l[0] not in data:
                data[l[0]] = int(l[1])
            else:
                data[l[0]] += int(l[1])
            oneline = iof.readline(MAX_LINE_LEN)
            oneline = oneline.rstrip()
    ioout = ""
    tnow = time()
    for val in data:
        ioout += METRIC_PREFIX + ".io %0.0f %0.0f image=%s type=%s\n" % (tnow, data[val], proc, val)
    return ioout


def getmem(proc, pidlist):
    total = 0; resident = 0
    for pid in pidlist:
        mempath = "/proc" + "/" + pid + "/" + "statm"
        try:
            memf = open(mempath, "r")
        except Exception, err:
            if DEBUG: stderr.write(mempath + " :%s\n" % str(err))
            continue
        oneline = memf.readline(MAX_LINE_LEN)
        oneline = oneline.split()
        total += int(oneline[0])
        resident += int(oneline[1])
    tnow = time()
    data = METRIC_PREFIX + ".mem %0.0f %0.0f image=%s type=total\n" % (tnow, total, proc)
    data += METRIC_PREFIX + ".mem %0.0f %0.0f image=%s type=resident\n" % (tnow, resident, proc)
    return data


def getfdcount(proc, pidlist):
    count = 0
    for pid in pidlist:
        try:
            count += len(os.listdir("/proc/%s/fd" % pid))
        except Exception, err:
            if DEBUG: stderr.write("/proc/%s/fd: %s\n" % (pid, str(err)))
            continue
    data = METRIC_PREFIX + ".fd %0.0f %0.0f image=%s type=open\n" % (time(), count, proc)
    return data


def getcputicktime(pid):
    try:
        fl = "/proc/" + pid + "/stat"
        cf = open(fl, "r")
    except Exception, err:
        if DEBUG: stderr.write("Failed to open %s: %s" % (fl, str(err)))
        return (None, None)
    info = cf.readline(MAX_LINE_LEN)
    info = info.split()
    # Add utime, stime, cutime and cstime. See man proc(5)
    ttime = int(info[13]) + int(info[14]) + int(info[15]) + int(info[16])
    return (ttime, time())


def getpcpu(proc, pid):
    (tt1, t1) = getcputicktime(pid)
    sleep(1)
    (tt2, t2) = getcputicktime(pid)
    if (t2 is None or t1 is None): return None
    t = (tt2 - tt1) / float(CLOCKTICK)
    pcpu = (t / (t2 - t1)) * 100
    return pcpu


def getallpcpu(procdict):
    inq = Queue()
    outq = Queue()
    threads = []
    for p in procdict:
        for ps in procdict[p]:
            inq.put([p, ps])
    for _ in range(THREADS):
        threads.append(Worker(inq, outq, getpcpu))
    for th in threads:
        th.join()
    allcpu = {}
    while not outq.empty():
        val = outq.get()
        if val[0] not in allcpu:
            allcpu[val[0]] = 0
        allcpu[val[0]] += val[1]
    data = ""
    tnow = time()
    for proc in allcpu:
        data += METRIC_PREFIX + ".pcpu %0.0f %0.1f image=%s\n" % (tnow, allcpu[proc], proc)
    return data


def getthreads(proc, pidlist):
    thcount = 0
    for p in pidlist:
        pth = "/proc/%s/stat" % p
        try:
            pf = open(pth, "r")
        except Exception, err:
            if DEBUG: stderr.write("Failed  to open %s: %s\n" % (pth, str(err)))
            continue
        cline = pf.readline(MAX_LINE_LEN)
        cline = cline.split()
        thcount += int(cline[19])
    data = METRIC_PREFIX + ".threads %0.0f %0.0f image=%s\n" % (time(), thcount, proc)
    return data


def summdict(d1, d2):
    if not d2:
        return d1
    for i in d1:
        d1[i] = d1[i] + d2[i]
    return d1


def getsockcount(pid, allconns):
    sockstat = dict()
    sockstat['tcp'] = 0
    sockstat['tcp6'] = 0
    sockstat['udp'] = 0
    sockstat['udp6'] = 0
    sockstat['unix'] = 0
    try:
        fds = os.listdir("/proc/%s/fd" % pid)
    except Exception, err:
        if DEBUG: stderr.write("getsocktype %s\n" % str(err))
        return sockstat
    sck = []
    for f in fds:
        try:
            s = os.readlink("/proc/%s/fd/%s" % (pid, f))
        except Exception, err:
            if DEBUG: stderr.write("getsocktype %s" % str(err))
            continue
        if match('socket', s):
            sck.append(search('[0-9]+', s).group(0))
    for i in sck:
        if i in allconns.tcpinodes: sockstat['tcp'] += 1
        elif i in allconns.tcpinodes: sockstat['tcp6'] += 1
        elif i in allconns.udpinodes: sockstat['udp'] += 1
        elif i in allconns.udp6inodes: sockstat['udp6'] += 1
        elif i in allconns.unixinodes: sockstat['unix'] += 1
    return sockstat


def getconns(procdict):
    try:
        tcpf = open("/proc/net/tcp", "r")
        udpf = open("/proc/net/udp", "r")
        unixf = open("/proc/net/unix", "r")
    except Exception, err:
        stderr.write("getconns %s\n" % str(err))
        return ""
    ip6 = True
    try:
        tcp6f = open("/proc/net/tcp6", "r")
        udp6f = open("/proc/net/udp6", "r")
    except Exception, err:
        stderr.write("getconns 6 %s\n" % str(err))
        ip6 = False
    allconns = ConnTable()
    for onel in tcpf:
        onel = onel.split()
        allconns.tcpinodes[onel[9]] = None
    for onel in udpf:
        onel = onel.split()
        allconns.udpinodes[onel[9]] = None
    for onel in unixf:
        onel = onel.split()
        allconns.unixinodes[onel[6]] = None
    if ip6:
        for onel in tcp6f:
            onel = onel.split()
            allconns.tcp6inodes[onel[9]] = None
        for onel in udp6f:
            onel = onel.split()
            allconns.udp6inodes[onel[9]] = None
    allsock = dict()
    for p in procdict:
        if p not in allsock:
            allsock[p] = dict()
        for ps in procdict[p]:
            s = getsockcount(ps, allconns)
            allsock[p] = summdict(s, allsock[p])
    data = ""
    tnow = time()
    for ps in allsock:
        for conn in allsock[ps]:
            data += METRIC_PREFIX + ".net %0.0f %s image=%s type=%s\n" % (tnow, allsock[ps][conn], ps, conn)
    return data


def putdata(procdict):
    for p in procdict:
        d = getio(p, procdict[p])
        print d
        d = getmem(p, procdict[p])
        print d
        d = getfdcount(p, procdict[p])
        print d
        d = getthreads(p, procdict[p])
        print d
    d = getallpcpu(procdict)
    print d
    d = getconns(procdict)
    print d
    return


def chkconf(proclist, procdict):
    for p in proclist:
        if p[0] not in procdict:
            stderr.write("%s %s specified in config file, but no  matches found \n" % (p[0], p[1].pattern))
    return

#################################################################
(proclist, oldtime, confpath) = getconf1()
while True:
    while proclist is None:
        stderr.write("%s contains no good config. Will try again in %d seconds.\n" % (confpath, CONF_WAIT))
        sleep(CONF_WAIT)
        (proclist, oldtime, confpath) = getconf1()
    procdict = getpids(proclist)
    if DEBUG: chkconf(proclist, procdict)
    putdata(procdict)
    try:
        newtime = os.stat(confpath).st_mtime
        if newtime > oldtime:
            stderr.write("Configuration changed reloading: %s\n" % confpath)
            (proclist, oldtime, confpath) = getconf1()
    except Exception, err:
        stderr.write("Failed to stat config file %s: %s\n" % (confpath, str(err)))
    stdout.flush()
    sleep(COLLECTION_INTERVAL)
