# coding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals

import datetime


CSV_COLUMNS = [
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
_CSV_COL_MAP = dict(CSV_COLUMNS)
_CSV_ORDER, _CSV_HEADER = zip(*CSV_COLUMNS)


def csv_row(event):
    for k in event:
        if isinstance(event[k], datetime.datetime):
            event[k] = event[k].isoformat()

    # guaranteed (if we're not using the raw masker...) there's no commas
    # anywhere in the data?
    return ','.join(str(event[k]) for k in _CSV_ORDER)


def write(filename, traces):
    with open(filename, 'w') as f:
        f.write(','.join(_CSV_HEADER) + '\n')

        for n, trace in enumerate(traces):
            # print(trace)
            f.write(csv_row(trace) + '\n')
