# This script creates tarball(s) from the tcollector project.
#
# The -base tarball contains the tcollector program and collectors that are run on every node (ie system-level metrics)
# the -role flag creates tarball for a given machine type.

import argparse
import os
import subprocess
import sys
import tempfile
import shutil

COLLECTORS = {'base': {'0': ['dfstat.py', 'ifstat.py', 'iostat.py', 'netstat.py', 'procnettcp.py', 'procstats.py', 'udp_bridge.py'], },
              'hadoop': {'0': ['hadoop_datanode_jmx.py', 'hadoop_master_jmx.py', 'hbase_regionserver_jmx.py'], '10': ['hbase_regionserver.py']},
              'flume': {'0': ['flume_jmx.py'] },
              'opentsdb': {'0': ['opentsdb.sh'] },
              'monitoring': {'0': ['cloudwatch.py'] },
}

ALL_ROLES = COLLECTORS.keys()

temp_dir = tempfile.mkdtemp()
tcollector_dir = temp_dir + '/tcollector'

collectors_dir = "collectors"

build_dir = "build"

def clear_symlinks():
    # walk through the tcollector/collectors/0/xxx directories and clears symlinks
    subprocess.call(['find', tcollector_dir + "/collectors", '-type', 'l', '-delete'])

def create_symlinks_for_role(role):
    # now create symlinks
    role_map = COLLECTORS[role]

    for freq in role_map: # will be numeric, ie 0, 30, etc
        freq_collectors_dir = "%s/%s" % (collectors_dir, freq)

        # make sure the directory exists
        base_dir = '%s/collectors/%s' % (tcollector_dir, freq)
        if not os.path.exists(base_dir):
            os.mkdir(base_dir)

        files = role_map[freq]
        for file in files:
            source = "%s/%s" % (freq_collectors_dir, file)
            name = '%s/%s' % (base_dir, file)
            os.symlink(source, name)

def create_tarball(role):
    filename = '%s/tcollector-%s.tar.gz' % (build_dir,role)
    subprocess.call(['tar', 'cfz', filename, '-C', temp_dir, 'tcollector'])


def create_tarball_for_role(role):
    create_symlinks_for_role('base')

    if role != 'base':
         create_symlinks_for_role(role)

    create_tarball(role)

def main():
    os.mkdir(tcollector_dir)
    os.mkdir("%s/collectors" % tcollector_dir)

    if not os.path.exists(build_dir):
        os.mkdir(build_dir)

    for role in ALL_ROLES:
        create_tarball_for_role(role)
        clear_symlinks()

    shutil.rmtree(tcollector_dir)

if __name__ == '__main__':
    main()
