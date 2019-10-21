# coding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals

import datetime
import logging

LOG = logging.getLogger(__name__)


CSV_INSTANCE_COLUMNS = [
    ('uuid', 'INSTANCE_UUID'),
    ('event', 'EVENT'),
    ('start_time', 'START_TIME'),
    ('start_sec', 'START_SEC'),
    ('finish_time', 'FINISH_TIME'),
    ('finish_sec', 'FINISH_SEC'),
    ('duration', 'EVENT_DURATION'),
    ('result', 'RESULT'),
    ('memory_mb', 'MEMORY_MB'),
    ('root_gb', 'DISK_GB'),
    ('vcpus', 'VCPUS'),
    ('hostname', 'INSTANCE_NAME'),
    ('user_id', 'USER_ID'),
    ('project_id', 'PROJECT_ID'),
    ('host', 'HOST_NAME (PHYSICAL)'),
]
_CSV_INSTANCE_COL_MAP = dict(CSV_INSTANCE_COLUMNS)
_CSV_INSTANCE_ORDER, _CSV_INSTANCE_HEADER = zip(*CSV_INSTANCE_COLUMNS)

_CSV_MACHINE_HEADER = ['EVENT_TIME', 
                       'EVENT_TIME_SEC', 
                       'HOST_NAME (PHYSICAL)', 
                       'RACK', 
                       'EVENT',
                       'VCPU_CAPACITY',
                       'MEMORY_CAPACITY_MB',
                       'DISK_CAPACITY_GB']

_HEADER = {'instance': _CSV_INSTANCE_HEADER,
           'machine': _CSV_MACHINE_HEADER}

_ORDER = {'instance': _CSV_INSTANCE_ORDER,
          'machine': _CSV_MACHINE_HEADER}


def csv_row(event, trace_type):
    for k in event:
        if isinstance(event[k], datetime.datetime):
            event[k] = event[k].isoformat()

    # guaranteed (if we're not using the raw masker...) there's no commas
    # anywhere in the data?
    return ','.join(str(event[k]) for k in _ORDER[trace_type])


def write(filename, traces, trace_type):
    with open(filename, 'w') as f:
        f.write(','.join(_HEADER[trace_type]) + '\n')

        for n, trace in enumerate(traces):
            # print(trace)
            line = csv_row(trace, trace_type)
            LOG.info(line)
            f.write(line + '\n')
