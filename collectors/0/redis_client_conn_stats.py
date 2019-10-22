# It statistics the client number of each redis intance
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


def save_redis_conn_info(conn_info):
    """
    save redis connection into B.
    For TCollector, you just need to output data to stdout
    :param conn_info : {ip:port: cnt}, {hostname: cnt}, {role: cnt}, total connection count
    :return:
    """
    _, host = get_current_ip_host()

    for key, cnt in conn_info[0].items():
        data = {
            'metric': 'redis.connected_client.ip',
            'timestamp': int(time.time()),
            'value': cnt,
            'tags': {
                'redis_host': host,
                'client_address': key,
            }
        }
        output = "%s %s %s %s" % (
            data['metric'], str(data['timestamp']), str(data['value']), build_tag_str(data['tags']))
        print output

    for key, cnt in conn_info[1].items():
        data = {
            'metric': 'redis.connected_client.host',
            'timestamp': int(time.time()),
            'value': cnt,
            'tags': {
                'redis_host': host,
                'client_host': key,
            }
        }
        output = "%s %s %s %s" % (
            data['metric'], str(data['timestamp']), str(data['value']), build_tag_str(data['tags']))
        print output

    for key, cnt in conn_info[2].items():
        data = {
            'metric': 'redis.connected_client.role',
            'timestamp': int(time.time()),
            'value': cnt,
            'tags': {
                'redis_host': host,
                'client_role': key,
            }
        }
        output = "%s %s %s %s" % (
            data['metric'], str(data['timestamp']), str(data['value']), build_tag_str(data['tags']))
        print output


def get_current_connection(conn):
    """
    return connection info of redis
    :return: {ip: cnt}, {hostname: cnt}, {role: cnt}, total connection count
    """
    addr_conn_info = {}
    host_conn_info = {}
    role_conn_info = {}
    clients = conn.client_list()
    mapping = get_ip_host_mapping()
    for client in clients:
        addr = client.get('addr')
        if not addr:
            continue

        ip_port = addr.split(':')
        if len(ip_port) != 2 or not ip_port[0]:
            continue
        if ip_port[0] in addr_conn_info:
            addr_conn_info[ip_port[0]] += 1
        else:
            addr_conn_info[ip_port[0]] = 1

        host = mapping.get(ip_port[0])
        if not host:
            continue
        idx = host.rfind('-')
        role = host[:idx] if idx > 0 else host
        if host in host_conn_info:
            host_conn_info[host] += 1
        else:
            host_conn_info[host] = 1

        if role in role_conn_info:
            role_conn_info[role] += 1
        else:
            role_conn_info[role] = 1
    return addr_conn_info, host_conn_info, role_conn_info, len(clients)


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


def map_add(map1, map2):
    for k, v in map2.items():
        if k in map1:
            map1[k] += v
        else:
            map1[k] = v


def main():
    while True:
        ports = get_all_redis_ports()
        info = [{}, {}, {}, 0]
        for port in ports:
            conn = redis.StrictRedis(host='127.0.0.1', port=port)
            conn_info = get_current_connection(conn)
            map_add(info[0], conn_info[0])
            map_add(info[1], conn_info[1])
            map_add(info[2], conn_info[2])
            info[3] += conn_info[3]
        save_redis_conn_info(info)
        time.sleep(60)


if __name__ == '__main__':
    sys.exit(main())
