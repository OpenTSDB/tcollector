#! /usr/bin/env python
'''
this module runs pylint on all python scripts found in a directory tree
'''

import os
import re
import sys
from optparse import OptionParser

total = 0.0
count = 0
errors = 0

VERBOSE = False
BASE_DIRECTORY = os.getcwd()
SUMMARY = False

class WritableObject(object):
    "dummy output stream for pylint"
    def __init__(self):
        self.content = []
    def write(self, st):
        "dummy write"
        self.content.append(st)
    def read(self):
        "dummy read"
        return self.content

def run_pylint(filename, options):
    "run pylint on the given file"
    ARGS = ["--rcfile=./.pylintrc"]  # put your own here
    if not options.show_all:
        ARGS.append("-E")
    pylint_output = WritableObject()
    from pylint import lint
    from pylint.reporters.text import TextReporter
    lint.Run([filename]+ARGS, reporter=TextReporter(pylint_output), exit=False)
    return pylint_output.read()

def print_line(line):
    global VERBOSE
    if VERBOSE:
        print(line.rstrip())

def check(module, options):
    '''
    apply pylint to the file specified if it is a *.py file
    '''
    global total, count, errors
    
    if module[-3:] == ".py":

        args = ''
        print("Checking %s" % (module))
        #print_line("CHECKING %s" % (module))
        pout = run_pylint(module, options)
        count += 1
        for line in pout:
            if re.match("E\:.*", line):
                errors += 1
                if options.summary or options.verbose:
                  print("Module: %s - %s" % (module, line.rstrip()))
            if re.match("[RCWF]\:.*", line) and options.show_all:
                print_line(line)
            if  re.match("E....:.", line):
                print_line(line)
            if "Your code has been rated at" in line:
                print_line(line)
                score = re.findall("\d.\d\d", line)[0]
                total += float(score)

def parse_cmdline(argv):
    """Parses the command-line."""
    global BASE_DIRECTORY, VERBOSE, SUMMARY

    DEFAULT_BASE_DIR = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])),
                                'collectors')

    parser = OptionParser(description='Runs pylint recursively on a directory')

    parser.add_option('-b', '--base-dir', dest='base_directory', metavar='DIR',
                      default=DEFAULT_BASE_DIR,
                      help='Directory to start linting')

    parser.add_option('-v', '--verbose', dest='verbose', action='store_true', default=False,
                      help='Verbose mode (log debug messages).')

    parser.add_option('-a', '--show-all', dest='show_all', action='store_true', default=False,
                      help='By default, we are only showing error lines')

    parser.add_option('-s', '--summary', dest='summary', action='store_true', default=False,
                      help='Show summary report')

    (options, args) = parser.parse_args(args=argv[1:])

    VERBOSE = options.verbose
    BASE_DIRECTORY = options.base_directory
    return (options, args)

def check_version():
  ver = sys.version_info
  if ver[0] == 2 and ver[1] < 7:
    sys.stderr.write("Requires Python  >2.7 for pylint\n")
    return False
  return True

def main(argv):
    global BASE_DIRECTORY, VERBOSE

    if not check_version():
      return 0

    options, args = parse_cmdline(argv)

    print_line("looking for *.py scripts in subdirectories of %s" % (BASE_DIRECTORY)) 

    for root, dirs, files in os.walk(BASE_DIRECTORY):
        for name in files:
            filepath = os.path.join(root, name)
            check(filepath, options)

    if options.summary:
        print("==" * 50)
        print("%d modules found" % count)
        print("%d errors found" % errors)
        if options.show_all and count > 0:
            print("AVERAGE SCORE = %.02f" % (total / count))
    return errors

if __name__ == '__main__':
    sys.exit(main(sys.argv))
