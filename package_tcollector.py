#! /usr/bin/env python2.7
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
              'hadoop': {'0': ['hadoop_datanode_jmx.py', 'hadoop_master_jmx.py'], '10': ['hbase_regionserver.py']},
              'flume': {'0': ['flume_jmx.py'] },
              'opentsdb': {'0': ['opentsdb.sh'] },
              'monitoring': {'0': ['cloudwatch.py'] },
}

ALL_ROLES = COLLECTORS.keys()

temp_dir = tempfile.mkdtemp() + "/tcollector"

collectors_dir = "collectors"

build_dir = "build"
tcollector_script = "tcollector.py"

def setup_tmp_directory():
    if os.path.exists(temp_dir): 
        shutil.rmtree(temp_dir)
    os.mkdir(temp_dir)
    os.mkdir("%s/collectors" % temp_dir)

def add_collectors_for_role(role):
    role_map = COLLECTORS[role]

    for freq in role_map: # will be numeric, ie 0, 30, etc
        freq_collectors_dir = "%s/%s" % (collectors_dir, freq)

        # make sure the directory exists
        base_dir = '%s/collectors/%s' % (temp_dir, freq)
        if not os.path.exists(base_dir):
            os.mkdir(base_dir)

        files = role_map[freq]
        for file in files:
            source = "%s/%s" % (freq_collectors_dir, file)
            name = '%s/%s' % (base_dir, file)
            shutil.copy(source, name)

def create_tarball(role):
    shutil.copy('tcollector.py', '%s/tcollector.py' % temp_dir)
    shutil.copytree('lib', '%s/lib' % temp_dir)

    filename = '%s/tcollector-%s.tar.gz' % (build_dir, role)
    subprocess.call(['tar', 'chfz', filename, '-C', temp_dir, '.'])


def create_tarball_for_role(role):
    add_collectors_for_role('base')

    if role != 'base':
        add_collectors_for_role(role)

    create_tarball(role)

def clean():
  if os.path.exists(build_dir):
    shutil.rmtree(build_dir)

def main():
    clean()
    setup_tmp_directory()

    if not os.path.exists(build_dir):
        os.mkdir(build_dir)

    for role in ALL_ROLES:
        create_tarball_for_role(role)
        setup_tmp_directory()

    shutil.rmtree(temp_dir)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print 'tmp directory: %s' % temp_dir
        import traceback
        traceback.print_exc()
