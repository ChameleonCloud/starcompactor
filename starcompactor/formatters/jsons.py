# coding: utf-8
import configparser
import datetime
import json
import logging

LOG = logging.getLogger(__name__)

config = configparser.ConfigParser()
config.read('starcompactor.config')

_CSV_PROPERTIES = {'instance': {'vm': ['memory_mb', 'root_gb', 'vcpus'], 'baremetal': []},
                   'machine': {'vm': ['rack', 'vcpu_capability', 'memory_capability_mb', 'disk_capability_gb'],
                               'baremetal': config.get('baremetal', 'properties').split(',')}}

def datetime_serializer(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("don't know how to serialize object {}".format(repr(obj)))


def write(filename, events, trace_type, instance_type):
    with open(filename, 'w') as f:
        for event in events:
            properties = {}
            for k in list(event.keys()):
                if k in _CSV_PROPERTIES[trace_type][instance_type]:
                    properties[k] = event[k]
                    del event[k]
            event['PROPERTIES'] = properties
            line = json.dumps(event, default=datetime_serializer)
            LOG.info(line)
            f.write(line + '\n')
