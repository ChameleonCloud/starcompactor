# coding: utf-8
def pipeline(iterator, *callables):
    for item in iterator:
        for f in callables:
            item = f(item)
        yield item
