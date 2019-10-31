# coding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals

import csv
import datetime
import logging

try:
    import configparser # 3.x
except ImportError:
    from backports import configparser # 2.x 3rd party

LOG = logging.getLogger(__name__)

config = configparser.ConfigParser()
config.read('starcompactor.config')

_CSV_INSTANCE_HEADER = ['INSTANCE_UUID',
                        'EVENT',
                        'START_TIME',
                        'START_SEC',
                        'FINISH_TIME',
                        'FINISH_SEC',
                        'EVENT_DURATION',
                        'RESULT',
                        'INSTANCE_NAME',
                        'USER_ID',
                        'PROJECT_ID',
                        'HOST_NAME (PHYSICAL)',
                        'PROPERTIES']

_CSV_MACHINE_HEADER = ['EVENT_TIME', 
                       'EVENT_TIME_SEC', 
                       'HOST_NAME (PHYSICAL)', 
                       'EVENT',
                       'PROPERTIES']

_CSV_PROPERTIES = {'instance': {'vm': ['memory_mb', 'root_gb', 'vcpus'], 'baremetal': []},
                   'machine': {'vm': ['rack', 'vcpu_capability', 'memory_capability_mb', 'disk_capability_gb'],
                               'baremetal': config.get('baremetal', 'properties').split(',')}}

_HEADER = {'instance': _CSV_INSTANCE_HEADER,
           'machine': _CSV_MACHINE_HEADER}


def csv_row(event, trace_type, instance_type):
    properties = {}
    for k in event.keys():
        if isinstance(event[k], datetime.datetime):
            event[k] = event[k].isoformat()
        if k in _CSV_PROPERTIES[trace_type][instance_type]:
            properties[k] = event[k]
            del event[k]
    event['PROPERTIES'] = properties

    return [str(event[k]) for k in _HEADER[trace_type]]


def write(filename, traces, trace_type, instance_type):
    with open(filename, 'w') as f:
        csvwriter = csv.writer(f, delimiter=b',', quotechar=b'"', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(_HEADER[trace_type])

        for n, trace in enumerate(traces):
            line = csv_row(trace, trace_type, instance_type)
            LOG.info(line)
            csvwriter.writerow(line)
