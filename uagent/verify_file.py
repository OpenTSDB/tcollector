#!/usr/bin/python2.7

import sys
import uagent

if (len(sys.argv) != 2):
    print("Usage: {0} <filename>".format(sys.argv[0]))
else:
    print(uagent.verify_file(sys.argv[1], None))
