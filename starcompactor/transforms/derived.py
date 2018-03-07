# coding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals

import logging

LOG = logging.getLogger(__name__)


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
