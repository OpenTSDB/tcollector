# !/usr/bin/env python
import json
import os
import sys
import time
import redis


def get_ip_host_mapping():
    """
    get ip address -> hostname mapping
    :return: {ip -> hostname}
    """
    ip_host_map = {}
    with open('/etc/hosts') as f:
        for line in f.readlines():
            line = line.strip()
            if line.startswith('#'):
                continue
            parts = line.split()
            if len(parts) != 3:
                continue
            ip_host_map[parts[0]] = parts[2]
    ip_host_map['127.0.0.1'] = 'localhost'
    return ip_host_map


def get_current_ip_host():
    """
    get current machine ip host information
    :return:
    """
    ip = os.popen('hostname -i').read().strip()
    host = get_ip_host_mapping().get(str(ip))
    return ip, host


def build_tag_str(tags):
    res = ''
    for key, val in tags.items():
        res += '%s=%s' % (key, val) + ' '
    return res[:len(res) - 1]


def get_all_redis_ports():
    res = os.popen('ps -ef | grep redis').read()
    lines = res.split('\n')
    ports = []
    for line in lines:
        if line.find('*:') <= 0:
            continue
        s = line.split('*:')[1][:6]
        port = ''
        for c in s:
            if c.isdigit():
                port += c
        ports.append(int(port))
    return ports


delimiters = [':', '-', '.', '_', '/']


def save_data_to_tsdb(host, port, ts):
    conn = redis.StrictRedis(host='127.0.0.1', port=port)
    cursor = 0
    print '=================== %s:%s ===================' % (host, port)
    # iterate through redis
    while True:
        cursor, keys = conn.scan(cursor=cursor, count=1000)
        for key in keys:
            size = conn.execute_command('MEMORY USAGE', key)
            prefix = key
            for deli in delimiters:
                if deli not in key:
                    continue
                else:
                    prefix = key.split(deli)[0]
                    break
            # save data into tsdb
            data = {
                'metric': 'redis.mem',
                'timestamp': ts,
                'value': float(size),
                'tags': {
                    'redis_host': host,
                    # Todo: OpenTSDB has some restriction for characters. Should have a better way to solve it.
                    'key': key.replace(':', '-').replace('=', '-').replace(',', '-'),
                    'prefix_key': prefix,
                }
            }
            output = "%s %s %s %s" % (
                data['metric'], str(data['timestamp']), str(data['value']), build_tag_str(data['tags']))
            print output
        if cursor == 0:
            break


def main():
    _, host = get_current_ip_host()
    if not host:
        host = 'None'
    while True:
        ts = int(time.time()) * 1000
        ports = get_all_redis_ports()
        for port in ports:
            save_data_to_tsdb(host, port, ts)

        time.sleep(60 * 60 * 24)  # 1 day


if __name__ == '__main__':
    sys.exit(main())
