# coding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals

def pipeline(iterator, *callables):
    for item in iterator:
        for f in callables:
            item = f(item)
        yield item
