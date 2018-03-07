# coding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals
import argparse
# import datetime
import functools
import itertools
import logging
import sys

from dateutil.parser import parse as dateparse

from .extractors import http
from .formatters import csv, jsons
from .transforms.derived import extra_times
from .transforms.masker import mask_fields, Masker, MASKERS


LOG = logging.getLogger(__name__)


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
             'it is vulnerable to cracking. "none" is for debugging.')
    parser.add_argument('-j', '--jsons', action='store_true',
        help='Format output as one JSON per line (defaults to CSV-style). '
             'Note that the file itself is *not* a JSON; read line-by-line '
             'and append them to an array for a proper JSON.')
    parser.add_argument('--osrc', type=str,
        help='RC file containing OS envvars.')
    parser.add_argument('-v', '--verbose',
        action='store_const', const=logging.INFO, dest="loglevel",
        help='Increase verbosity about the dump.')
    parser.add_argument('--debug',
        action='store_const', const=logging.DEBUG, dest="loglevel",
        help='Debug-level logging.')
    parser.add_argument('output_file', type=str,
        help='File to dump results')

    args = parser.parse_args()
    if args.loglevel is None:
        args.loglevel = logging.WARNING
    logging.basicConfig(level=args.loglevel)

    # start = dateparse(args.start) if args.start else None
    # end = dateparse(args.end) if args.end else None
    epoch = dateparse(args.epoch)
    LOG.debug('epoch time: {}'.format(epoch))

    masker_config = MASKERS[args.masking]
    LOG.debug('using masker options {}'.format(masker_config))
    mask = Masker(**masker_config)

    auth = http.Auth.from_env_or_args(env=True, args=args)

    traces = http.traces(auth)
    traces = pipeline(traces,
        functools.partial(mask_fields, masker=mask),
        functools.partial(extra_times, epoch=epoch),
    )

    #traces = itertools.islice(traces, 20)
    if args.jsons:
        LOG.debug('writing JSONs to {}'.format(args.output_file))
        jsons.write(args.output_file, traces)
    else:
        LOG.debug('writing CSV to {}'.format(args.output_file))
        csv.write(args.output_file, traces)


if __name__ == '__main__':
    sys.exit(main())
