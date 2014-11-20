#!/usr/bin/python

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
# I have modified this Python Ping Implementation Program 

""" About The PROGRAM : THe Following Python Program checks the Ping Time from your system [where you are running the tcollector] and to the 
destination IP(s) you have mentioned in the config.txt file. 

The config.txt file for this program resides in ../tcollector/collectos/etc/config.txt 

NOTE : Make Sure you mention only the IP's and not the domain name : 

SAMPLE CONFIG File exists. """


"""
    [SOME PART OF THE PROGRAM HAS BEEN TAKEN FROM THE INTERNET. I AM KEEPING THE CITATION INTACT - ANIKET MAITHANI]

    A pure python ping implementation using raw socket.
 
 
    Note that ICMP messages can only be sent from processes running as root.
 
 
    Derived from ping.c distributed in Linux's netkit. That code is
    copyright (c) 1989 by The Regents of the University of California.
    That code is in turn derived from code written by Mike Muuss of the
    US Army Ballistic Research Laboratory in December, 1983 and
    placed in the public domain. They have my thanks.
 
    Bugs are naturally mine. I'd be glad to hear about them. There are
    certainly word - size dependenceies here.
 
    Copyright (c) Matthew Dixon Cowles, <http://www.visi.com/~mdc/>.
    Distributable under the terms of the GNU General Public License
    version 2. Provided with no warranties of any sort.
 
    Original Version from Matthew Dixon Cowles:
      -> ftp://ftp.visi.com/users/mdc/ping.py
 
    Rewrite by Jens Diemer:
      -> http://www.python-forum.de/post-69122.html#69122
 
    Rewrite by George Notaras:
      -> http://www.g-loaded.eu/2009/10/30/python-ping/
 
    Revision history
    ~~~~~~~~~~~~~~~~
 
    November 8, 2009
    ----------------
    Improved compatibility with GNU/Linux systems.
 
    Fixes by:
     * George Notaras -- http://www.g-loaded.eu
    Reported by:
     * Chris Hallman -- http://cdhallman.blogspot.com
 
    Changes in this release:
     - Re-use time.time() instead of time.clock(). The 2007 implementation
       worked only under Microsoft Windows. Failed on GNU/Linux.
       time.clock() behaves differently under the two OSes[1].
 
    [1] http://docs.python.org/library/time.html#time.clock
 
    May 30, 2007
    ------------
    little rewrite by Jens Diemer:
     -  change socket asterisk import to a normal import
     -  replace time.time() with time.clock()
     -  delete "return None" (or change to "return" only)
     -  in checksum() rename "str" to "source_string"
 
    November 22, 1997
    -----------------
    Initial hack. Doesn't do much, but rather than try to guess
    what features I (or others) will want in the future, I've only
    put in what I need now.
 
    December 16, 1997
    -----------------
    For some reason, the checksum bytes are in the wrong order when
    this is run under Solaris 2.X for SPARC but it works right under
    Linux x86. Since I don't know just what's wrong, I'll swap the
    bytes always and then do an htons().
 
    December 4, 2000
    ----------------
    Changed the struct.pack() calls to pack the checksum and ID as
    unsigned. My thanks to Jerome Poincheval for the fix.
 
 
    Last commit info:
    ~~~~~~~~~~~~~~~~~
    $LastChangedDate: $
    $Rev: $
    $Author: $
"""
 

import os, sys, socket, struct, select, time
from collectors.lib import utils
COLLECTION_INTERVAL = 1  # default value as stated to me in seconds
metric ="rtt.rto"
hoster=os.popen('hostname -f').read()


ICMP_ECHO_REQUEST = 8
def main():
    with open('config.txt') as fp:
        for line in fp:
            ip=line
            a=int(time.time())
            def checksum(source_string):

                sum = 0
                countTo = (len(source_string)/2)*2
                count = 0
                while count<countTo:
                    thisVal = ord(source_string[count + 1])*256 + ord(source_string[count])
                    sum = sum + thisVal
                    sum = sum & 0xffffffff
                    count = count + 2

                if countTo<len(source_string):
                    sum = sum + ord(source_string[len(source_string) - 1])
                    sum = sum & 0xffffffff

                sum = (sum >> 16)  +  (sum & 0xffff)
                sum = sum + (sum >> 16)
                answer = ~sum
                answer = answer & 0xffff


                answer = answer >> 8 | (answer << 8 & 0xff00)

                return answer


            def receive_one_ping(my_socket, ID, timeout):

                timeLeft = timeout
                while True:
                    startedSelect = time.time()
                    whatReady = select.select([my_socket], [], [], timeLeft)
                    howLongInSelect = (time.time() - startedSelect)
                    if whatReady[0] == []:
                        return

                    timeReceived = time.time()
                    recPacket, addr = my_socket.recvfrom(1024)
                    icmpHeader = recPacket[20:28]
                    type, code, checksum, packetID, sequence = struct.unpack(
                        "bbHHh", icmpHeader
                    )
                    if packetID == ID:
                        bytesInDouble = struct.calcsize("d")
                        timeSent = struct.unpack("d", recPacket[28:28 + bytesInDouble])[0]
                        return timeReceived - timeSent

                    timeLeft = timeLeft - howLongInSelect
                    if timeLeft <= 0:
                        return


            def send_one_ping(my_socket, dest_addr, ID):

                dest_addr  =  socket.gethostbyname(dest_addr)

                # Header is type (8), code (8), checksum (16), id (16), sequence (16)
                my_checksum = 0

                # Make a dummy heder with a 0 checksum.
                header = struct.pack("bbHHh", ICMP_ECHO_REQUEST, 0, my_checksum, ID, 1)
                bytesInDouble = struct.calcsize("d")
                data = (192 - bytesInDouble) * "Q"
                data = struct.pack("d", time.time()) + data

                # Calculate the checksum on the data and the dummy header.
                my_checksum = checksum(header + data)


                header = struct.pack(
                    "bbHHh", ICMP_ECHO_REQUEST, 0, socket.htons(my_checksum), ID, 1
                )
                packet = header + data

                my_socket.sendto(packet, (dest_addr, 1))

            def do_one(dest_addr, timeout):

                icmp = socket.getprotobyname("icmp")
                try:
                    my_socket = socket.socket(socket.AF_INET, socket.SOCK_RAW, icmp)
                except socket.error, (errno, msg):
                    if errno == 1:
                        # Operation not permitted
                        msg = msg + (
                            " - Note that ICMP messages can only be sent from processes"
                            " running as root."
                        )
                        raise socket.error(msg)
                    raise # raise the original error

                my_ID = os.getpid() & 0xFFFF

                send_one_ping(my_socket, dest_addr, my_ID)
                delay = receive_one_ping(my_socket, my_ID, timeout)

                my_socket.close()
                return delay


            def verbose_ping(dest_addr, timeout = 2, count = 4):

                for i in xrange(count):
                    
                    try:
                        delay  =  do_one(dest_addr, timeout)
                    except socket.gaierror, e:
                        print "failed. (socket error: '%s')" % e[1]
                        break

                    if delay  ==  None:
                        print "failed. (timeout within %ssec.)" % timeout
                    else:
                        delay  =  delay * 1000
                        b=int(time.time())
                       
                        print ("%s %d %f dest=%s \n"  %(metric,b,delay,ip))
                        sys.stdout.flush()
                        time.sleep(COLLECTION_INTERVAL)

                


            if __name__ == '__main__':
                verbose_ping(ip)

if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main())

