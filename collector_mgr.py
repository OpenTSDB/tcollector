#!/usr/bin/env python

import os
import glob
import sys
import ConfigParser


def main(argv):
    agent_install_root = os.path.dirname(os.path.realpath(__file__))
    global COLLECTOR_CONFIG_ROOT
    COLLECTOR_CONFIG_ROOT = os.path.join(agent_install_root, "collectors/conf")
    if len(argv) < 2:
        usage_and_exit(argv)
    if argv[1] == 'list':
        status()
    elif argv[1] == 'version':
        version()
    else:
        if argv[1] == 'enable':
            if len(argv) < 3:
                usage_and_exit(argv)
            enable_or_disable(argv[2], "True")
        elif argv[1] == 'disable':
            if len(argv) < 3:
                usage_and_exit(argv)
            enable_or_disable(argv[2], "False")
        else:
            usage_and_exit(argv)


def usage_and_exit(argv):
    print "Usage: %s list|enable|disable <conf_name, e.g.tomcat>" % os.path.basename(argv[0])
    sys.exit(1)


def status():
    results = []
    conf_files = glob.glob(os.path.join(COLLECTOR_CONFIG_ROOT, "*.conf"))
    for conf_file in conf_files:
        config = ConfigParser.SafeConfigParser({"enabled": 'False', 'interval': '15'})
        config.read(conf_file)
        enabled_str = config.get("base", "enabled")
        enabled = 0 if enabled_str == "True" else 1
        interval = config.get("base", "interval")
        results.append((enabled, os.path.splitext(os.path.basename(conf_file))[0], enabled_str, interval))
    results.sort()

    for item in results:
        print "{: >20} {: >10} {: >5}".format(*item[1:])


def enable_or_disable(comma_demilitted_str, action):
    for conf_file_name in comma_demilitted_str.split(","):
        try:
            conf_file_path = os.path.join(COLLECTOR_CONFIG_ROOT, conf_file_name + ".conf")
            parser = ConfigParser.SafeConfigParser()
            parser.read(conf_file_path)
            parser.set("base", "enabled", action)
            with open(conf_file_path, 'wb') as fp:
                parser.write(fp)
        except Exception as e:
            print 'error enable %s. %s' % (conf_file_name, e)
    print 'done'
    status()

def version():
    conf_file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "runner.conf")
    config = ConfigParser.SafeConfigParser({"commit": 'null', 'version': 'null'})
    config.read(conf_file_path)
    print "Cloudwiz Collector Version: " + config.get('base', 'version')
    print "release commit code: " + config.get('base', 'commit')

if __name__ == '__main__':
    sys.exit(main(sys.argv))
