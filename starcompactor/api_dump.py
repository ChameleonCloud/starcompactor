# coding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals
import argparse
# import datetime
import functools
import itertools
import sys

from dateutil.parser import parse as dateparse

from .extractors import http
from .formatters import csv, jsons
from .transforms.derived import extra_times
from .transforms.masker import mask_fields, Masker, MASKERS


def pipeline(iterator, *callables):
    for item in iterator:
        for f in callables:
            item = f(item)
        yield item


def main():
    parser = argparse.ArgumentParser(description=__doc__)

    # parser.add_argument('-s', '--start', type=str)
    # parser.add_argument('-e', '--end', type=str)
    parser.add_argument('-c', '--epoch', type=str, default='2015-09-06T23:31:16',
        help='Time to compute relative dates from')
    parser.add_argument('-m', '--masking', type=str, default='sha2-salted',
        choices=MASKERS,
        help='User data mask type. "sha1-raw" is legacy and not recommended as '
             'it is vulnerable to cracking. "none" is for debugging only.')
    parser.add_argument('-j', '--jsons', action='store_true',
        help='Format output as one JSON per line (defaults to CSV-style)')
    parser.add_argument('output_file', type=str,
        help='File to dump results')

    args = parser.parse_args()

    # start = dateparse(args.start) if args.start else None
    # end = dateparse(args.end) if args.end else None
    epoch = dateparse(args.epoch)
    mask = Masker(**MASKERS[args.masking])

    auth = http.Auth.from_env_or_args(env=True)

    traces = http.traces(auth)
    traces = pipeline(traces,
        functools.partial(mask_fields, masker=mask),
        functools.partial(extra_times, epoch=epoch),
    )

    if args.jsons:
        pass
    else:
        csv.write(args.output_file, itertools.islice(traces, 20))


if __name__ == '__main__':
    sys.exit(main())
