# coding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals
import os
import hashlib

__all__ = ['MASKED_FIELDS', 'MASKERS', 'Masker', 'mask_fields', 'ordered_mask']

MASKED_FIELDS = {'instance': ['INSTANCE_UUID', 'USER_ID', 'PROJECT_ID', 'INSTANCE_NAME', 'HOST_NAME (PHYSICAL)'],
                 'machine': ['HOST_NAME (PHYSICAL)']}
MASKERS = {
    'none': {'method': 'raw'}, # debugging
    'sha1-raw': {'method': 'sha1', 'salt': b''}, # legacy
    'sha2-salted': {'method': 'sha256', 'truncate': 32}, # default
}


class Masker:
    '''
    Generates a callable that will hash data so the original is not known. A
    salt is recommended to prevent reversing low-entropy values.

    Returns a hex string.

    Keyword Arguments
    -----------------
    If *salt* is ``None``, a 256-bit salt is generated. Storing it is not
    recommended.

    If *truncate* is not ``None``, the resulting hex string is truncated to
    that many digits. If the number of unique hashes is less than 20 billion,
    128-bit hashes collide with the probability $10^{-18}$.
    '''
    def __init__(self, method='sha256', salt=None, truncate=None):
        if salt is None:
            self.salt = os.urandom(32)
        else:
            self.salt = salt
        self.method = method
        self.truncate = truncate

    def __call__(self, data):
        if self.method == 'raw':
            return data[:self.truncate]
        h = hashlib.new(self.method, self.salt)
        h.update(data.encode('utf-8'))
        return h.hexdigest()[:self.truncate]


def mask_fields(trace, trace_type, masker):
    for field in MASKED_FIELDS[trace_type]:
        if trace[field]:
            trace[field] = masker(trace[field])
        else:
            trace[field] = ''
    return trace

def ordered_mask(trace, field_name, ordered_list):
    field_value = trace.setdefault(field_name, None)
    if field_value:
        try:
            trace[field_name] = ordered_list.index(field_value)
        except ValueError:
            trace[field_name] = None
        
    return trace
    
