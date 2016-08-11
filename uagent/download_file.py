#!/usr/bin/python2.7

import sys
import uagent

if (len(sys.argv) != 3):
    print("Usage: {0} <url> <size-limit-bytes>".format(sys.argv[0]))
else:
    print(uagent.download_file(sys.argv[1], int(sys.argv[2])))
