#!/usr/bin/env /opt/cloudwiz-agent/altenv/bin/python

import json
import time
import urllib2

es_url = "http://10.9.126.97:9200/_nodes/stats"
response = urllib2.urlopen(es_url).read()

ts = int(time.time())
parsed = json.loads(response)

cluster = parsed['cluster_name'].lower()
#print parsed['nodes']

for node in parsed['nodes']:

    # collecting metrics

    name = parsed['nodes'][node]['name'].lower()
    host = parsed['nodes'][node]['host'].lower().replace('.', '_')

    indices = parsed['nodes'][node]['indices']

    docs_count = indices['docs']['count']
    bytes_count = indices['store']['size_in_bytes']
    throttle_ms = indices['store']['throttle_time_in_millis']

    index_total = indices['indexing']['index_total']
    index_ms = indices['indexing']['index_time_in_millis']
    index_avg = index_ms / index_total
    index_current = indices['indexing']['index_current']
    index_failed = indices['indexing']['index_failed']

    query_current = indices['search']['query_current']
    query_total = indices['search']['query_total']
    query_ms = indices['search']['query_time_in_millis']
    query_avg = query_ms / query_total
    fetch_current = indices['search']['fetch_current']
    fetch_total = indices['search']['fetch_total']
    fetch_ms = indices['search']['fetch_time_in_millis']
    fetch_avg = fetch_ms / fetch_total

    fielddata_size = indices['fielddata']['memory_size_in_bytes']
    fielddata_evictions = indices['fielddata']['evictions']
    query_cache_bytes = indices['query_cache']['memory_size_in_bytes']
    query_evictions = indices['query_cache']['evictions']

    jvm = parsed['nodes'][node]['jvm']

    young_count = jvm['gc']['collectors']['young']['collection_count']
    young_ms = jvm['gc']['collectors']['young']['collection_time_in_millis']
    young_avg = young_ms / young_count
    old_count = jvm['gc']['collectors']['old']['collection_count']
    old_ms = jvm['gc']['collectors']['old']['collection_time_in_millis']
    old_avg = old_ms / old_count

    heap_used_pct = jvm['mem']['heap_used_percent']
    heap_committed = jvm['mem']['heap_committed_in_bytes']

    os = parsed['nodes'][node]['os']

    cpu_pct = os['cpu_percent']
    cpu_load = os['load_average']
    mem_swap_used = os['swap']['used_in_bytes']

    fs = parsed['nodes'][node]['fs']

    #fs_avail = fs['data']['available_in_bytes']

    http = parsed['nodes'][node]['http']

    http_open = http['current_open']

    thread = parsed['nodes'][node]['thread_pool']

    bulk_queue = thread['bulk']['queue']
    bulk_rejected = thread['bulk']['rejected']
    flush_queue = thread['flush']['queue']
    flush_rejected = thread['flush']['rejected']
    index_queue = thread['index']['queue']
    index_rejected = thread['index']['rejected']
    listener_queue = thread['listener']['queue']
    listener_rejected = thread['listener']['rejected']
    mgmt_queue = thread['management']['queue']
    mgmt_rejected = thread['management']['rejected']
    search_queue = thread['search']['queue']
    search_rejected = thread['search']['rejected']

    # print metrics to stdout
    print 'elasticsearch.{0}.nodes.indices.docs_count {1} {2} host={3} node={4}'.format(cluster, ts, docs_count, host, name)
    print 'elasticsearch.{0}.nodes.indices.bytes_count {1} {2} host={3} node={4}'.format(cluster, ts, bytes_count, host, name)
    print 'elasticsearch.{0}.nodes.indices.throttle_ms {1} {2} host={3} node={4}'.format(cluster, ts, throttle_ms, host, name)
    print 'elasticsearch.{0}.nodes.indices.index_total {1} {2} host={3} node={4}'.format(cluster, ts, index_total, host, name)
    print 'elasticsearch.{0}.nodes.indices.index_avg {1} {2} host={3} node={4}'.format(cluster, ts, index_avg, host, name)
    print 'elasticsearch.{0}.nodes.indices.index_current {1} {2} host={3} node={4}'.format(cluster, ts, index_current, host, name)
    print 'elasticsearch.{0}.nodes.indices.index_failed {1} {2} host={3} node={4}'.format(cluster, ts, index_failed, host, name)
    print 'elasticsearch.{0}.nodes.indices.query_current {1} {2} host={3} node={4}'.format(cluster, ts, query_current, host, name)
    print 'elasticsearch.{0}.nodes.indices.query_total {1} {2} host={3} node={4}'.format(cluster, ts, query_total, host, name)
    print 'elasticsearch.{0}.nodes.indices.query_avg {1} {2} host={3} node={4}'.format(cluster, ts, query_avg, host, name)
    print 'elasticsearch.{0}.nodes.indices.fetch_current {1} {2} host={3} node={4}'.format(cluster, ts, fetch_current, host, name)
    print 'elasticsearch.{0}.nodes.indices.fetch_total {1} {2} host={3} node={4}'.format(cluster, ts, fetch_total, host, name)
    print 'elasticsearch.{0}.nodes.indices.fetch_avg {1} {2} host={3} node={4}'.format(cluster, ts, fetch_avg, host, name)
    print 'elasticsearch.{0}.nodes.indices.fielddata_size {1} {2} host={3} node={4}'.format(cluster, ts, fielddata_size, host, name)
    print 'elasticsearch.{0}.nodes.indices.fielddata_evictions {1} {2} host={3} node={4}'.format(cluster, ts, fielddata_evictions, host, name)
    print 'elasticsearch.{0}.nodes.indices.query_cache_bytes {1} {2} host={3} node={4}'.format(cluster, ts, query_cache_bytes, host, name)
    print 'elasticsearch.{0}.nodes.indices.query_evictions {1} {2} host={3} node={4}'.format(cluster, ts, query_evictions, host, name)
    print 'elasticsearch.{0}.nodes.jvm.young_count {1} {2} host={3} node={4}'.format(cluster, ts, young_count, host, name)
    print 'elasticsearch.{0}.nodes.jvm.young_avg {1} {2} host={3} node={4}'.format(cluster, ts, young_avg, host, name)
    print 'elasticsearch.{0}.nodes.jvm.old_count {1} {2} host={3} node={4}'.format(cluster, ts, old_count, host, name)
    print 'elasticsearch.{0}.nodes.jvm.old_avg {1} {2} host={3} node={4}'.format(cluster, ts, old_avg, host, name)
    print 'elasticsearch.{0}.nodes.jvm.heap_used_pct {1} {2} host={3} node={4}'.format(cluster, ts, heap_used_pct, host, name)
    print 'elasticsearch.{0}.nodes.jvm.heap_committed {1} {2} host={3} node={4}'.format(cluster, ts, heap_committed, host, name)
    print 'elasticsearch.{0}.nodes.os.cpu_pct {1} {2} host={3} node={4}'.format(cluster, ts, cpu_pct, host, name)
    print 'elasticsearch.{0}.nodes.os.cpu_load {1} {2} host={3} node={4}'.format(cluster, ts, cpu_load, host, name)
    print 'elasticsearch.{0}.nodes.os.mem_swap_used {1} {2} host={3} node={4}'.format(cluster, ts, mem_swap_used, host, name)
    print 'elasticsearch.{0}.nodes.http.current_open {1} {2} host={3} node={4}'.format(cluster, ts, http_open, host, name)
    print 'elasticsearch.{0}.nodes.thread.bulk_queue {1} {2} host={3} node={4}'.format(cluster, ts, bulk_queue, host, name)
    print 'elasticsearch.{0}.nodes.thread.bulk_rejected {1} {2} host={3} node={4}'.format(cluster, ts, bulk_rejected, host, name)
    print 'elasticsearch.{0}.nodes.thread.flush_queue {1} {2} host={3} node={4}'.format(cluster, ts, flush_queue, host, name)
    print 'elasticsearch.{0}.nodes.thread.flush_rejected {1} {2} host={3} node={4}'.format(cluster, ts, flush_rejected, host, name)
    print 'elasticsearch.{0}.nodes.thread.index_queue {1} {2} host={3} node={4}'.format(cluster, ts, index_queue, host, name)
    print 'elasticsearch.{0}.nodes.thread.index_rejected {1} {2} host={3} node={4}'.format(cluster, ts, index_rejected, host, name)
    print 'elasticsearch.{0}.nodes.thread.listener_queue {1} {2} host={3} node={4}'.format(cluster, ts, listener_queue, host, name)
    print 'elasticsearch.{0}.nodes.thread.listener_rejected {1} {2} host={3} node={4}'.format(cluster, ts, listener_rejected, host, name)
    print 'elasticsearch.{0}.nodes.thread.mgmt_queue {1} {2} host={3} node={4}'.format(cluster, ts, mgmt_queue, host, name)
    print 'elasticsearch.{0}.nodes.thread.mgmt_rejected {1} {2} host={3} node={4}'.format(cluster, ts, mgmt_rejected, host, name)
    print 'elasticsearch.{0}.nodes.thread.search_queue {1} {2} host={3} node={4}'.format(cluster, ts, search_queue, host, name)
    print 'elasticsearch.{0}.nodes.thread.search_rejected {1} {2} host={3} node={4}'.format(cluster, ts, search_rejected, host, name)
