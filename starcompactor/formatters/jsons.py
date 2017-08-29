# coding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals

import datetime
import json


def datetime_serializer(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("don't know how to serialize object {}".format(repr(obj)))


def write(filename, events):
    with open(filename, 'w') as f:
        for event in events:
            f.write(json.dumps(event) + '\n')
