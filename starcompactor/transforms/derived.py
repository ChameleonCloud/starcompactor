# coding: utf-8
import logging

LOG = logging.getLogger(__name__)

__all__ = ['extra_times', 'machine_event_times']

def extra_times(trace, epoch):
    if trace['START_TIME']:
        # strange, but it's happening in the data...
        trace['START_SEC'] = (trace['START_TIME'] - epoch).total_seconds()
    else:
        LOG.debug('trace missing start_time')
        trace['START_SEC'] = None

    if trace['FINISH_TIME']:
        trace['FINISH_SEC'] = (trace['FINISH_TIME'] - epoch).total_seconds()
    else:
        LOG.debug('trace missing finish_time')
        trace['FINISH_SEC'] = None

    if trace['START_TIME'] and trace['FINISH_TIME']:
        trace['EVENT_DURATION'] = (trace['FINISH_TIME'] - trace['START_TIME']).total_seconds()
    else:
        trace['EVENT_DURATION'] = None

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
