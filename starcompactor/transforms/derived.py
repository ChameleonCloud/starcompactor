# coding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals

import logging

LOG = logging.getLogger(__name__)

__all__ = ['extra_times', 'machine_event_times']

def extra_times(trace, epoch):
    if trace['start_time']:
        # strange, but it's happening in the data...
        trace['start_sec'] = (trace['start_time'] - epoch).total_seconds()
    else:
        LOG.debug('trace missing start_time')
        trace['start_sec'] = None

    if trace['finish_time']:
        trace['finish_sec'] = (trace['finish_time'] - epoch).total_seconds()
    else:
        LOG.debug('trace missing finish_time')
        trace['finish_sec'] = None

    if trace['start_time'] and trace['finish_time']:
        trace['duration'] = (trace['finish_time'] - trace['start_time']).total_seconds()
    else:
        trace['duration'] = None

    return trace

def machine_event_times(trace, epoch):
    if trace['EVENT_TIME']:
        trace['EVENT_TIME_SEC'] = (trace['EVENT_TIME'] - epoch).total_seconds()
        if trace['EVENT_TIME_SEC'] < 0:
            trace['EVENT_TIME_SEC'] = -1
    else:
        LOG.debug('trace missing event_time')
        trace['EVENT_TIME_SEC'] = None
        
    return trace
