#!/usr/bin/env python


def enabled():
    return False


def get_config():
    # config = {
    #     ## interval in seconds to fetch metrics
    #     'interval': 20,
    #     ## List of tags to be added to all metrics
    #     'common_tags': {
    #         'env': 'prd'
    #     },
    #     ## For each instance of Jolokia, fetch this list of mbeans
    #     ## mbean parameter keys MUST be in alphanumeric order
    #     ##      eg: name, type
    #     'common_monitors': [{
    #         'mbean': 'java.lang:type=*',
    #         'metric': 'java.lang',
    #         'not_tags': ['type']
    #         }, {
    #         'mbean': 'java.lang:type=Runtime',
    #         'metric': 'java.lang',
    #         'not_tags': ['type']
    #         }, {
    #         'mbean': 'java.lang:name=*,type=GarbageCollector',
    #         'metric': 'java.lang',
    #         'not_tags': ['type']
    #         }, {
    #         'mbean': 'java.lang:name=*,type=MemoryPool',
    #         'metric': 'java.lang',
    #         'not_tags': ['type']
    #     }],
    #     ## List of instances of Jolokia to query for metrics
    #     'instances': [{
    #         ## url: required
    #         'url': 'http://localhost:8778/jolokia/',
    #         ## optional basic auth credentials
    #         'auth': {
    #             'username': 'adminRole',
    #             'password': 'passwordhere'
    #         }
    #         ## list of additional tags for this instance
    #         'tags': {
    #             'cluster': 'cluster01'
    #         },
    #         ## list of additional mbeans to monitor for this instance
    #         'monitors': [{
    #             'mbean': 'org.apache.cassandra.*:*',
    #             'metric': 'cassandra.metrics',
    #             'not_tags': ['type', 'name']
    #         }]
    #     }, {
    #         'url': 'http://localhost:8998/jolokia/',
    #         'tags': {
    #             'cluster': 'cluster02'
    #         },
    #         'monitors': [{
    #             'mbean': 'org.apache.zookeeper.*:*',
    #             'metric': 'zookeeper.metrics',
    #             'not_tags': ['type', 'name']
    #         }]
    #     }]
    # }

    config = {
        'interval': 20,
        'common_monitors': [{
            'mbean': 'java.lang:type=*',
            'metric': 'java.lang',
            'not_tags': ['type']
            }, {
            'mbean': 'java.lang:type=Runtime',
            'metric': 'java.lang',
            'not_tags': ['type']
            }, {
            'mbean': 'java.lang:name=*,type=GarbageCollector',
            'metric': 'java.lang',
            'not_tags': ['type']
            }, {
            'mbean': 'java.lang:name=*,type=MemoryPool',
            'metric': 'java.lang',
            'not_tags': ['type']
        }],
        'instances': [{
            'url': 'http://localhost:8778/jolokia/'
        }]
    }

    return config
