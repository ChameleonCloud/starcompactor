# coding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals


def extra_times(trace, epoch):
    trace['start_sec'] = (trace['start_time'] - epoch).total_seconds()
    if trace['finish_time'] is not None:
        trace['finish_sec'] = (trace['finish_time'] - epoch).total_seconds()
        trace['duration'] = (trace['finish_time'] - trace['start_time']).total_seconds()
    else:
        trace['finish_sec'] = None
        trace['duration'] = None
    return trace
