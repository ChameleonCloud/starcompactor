# coding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals
import argparse
import datetime
import functools
import itertools
import json
import logging
import sys

from dateutil.parser import parse as dateparse

from . import transforms as trans
from .extractors import mysql
from .formatters import csv, jsons
from .util import pipeline, Constants

TRACE_TYPE = 'instance'

LOG = logging.getLogger(__name__)


def datetime_serializer(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("don't know how to serialize object {}".format(repr(obj)))


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    mysqlargs = mysql.MySqlArgs({
        'user': 'root',
        'password': '',
        'host': 'localhost',
        'port': 3306,
    })
    mysqlargs.inject(parser)
    parser.add_argument('-d', '--database', type=str,
        help='Database name')
    parser.add_argument('-s', '--start', type=str)
    parser.add_argument('-e', '--end', type=str)
    parser.add_argument('-c', '--epoch', type=str, default=Constants.CHAMELEON_KVM_START_DATE + 'T00:00:00',
        help='Time to compute relative dates from')
    parser.add_argument('-m', '--masking', type=str, default='sha2-salted',
        choices=trans.MASKERS,
        help='User data mask type. "sha1-raw" is legacy and not recommended as '
             'it is vulnerable to cracking. "none" is for debugging only.')
    parser.add_argument('-t', '--salt', type=str, default=None,
        help = 'Salt of hashed masking method. Please use the same salt for machine event host name! '
               'Ignored if masking method is "none".')
    parser.add_argument('-j', '--jsons', action='store_true',
        help='Format output as one JSON per line (defaults to CSV-style)')
    parser.add_argument('-v', '--verbose',
        action='store_const', const=logging.INFO, dest="loglevel",
        help='Increase verbosity about the dump.')
    parser.add_argument('--debug',
        action='store_const', const=logging.DEBUG, dest="loglevel",
        help='Debug-level logging.')
    parser.add_argument('output_file', type=str,
        help='File to dump results')

    args = parser.parse_args(argv[1:])
    mysqlargs.extract(args)

    start = dateparse(args.start) if args.start else None
    end = dateparse(args.end) if args.end else None

    args = parser.parse_args()
    if args.loglevel is None:
        args.loglevel = logging.WARNING
    logging.basicConfig(level=args.loglevel)

    epoch = dateparse(args.epoch)
    LOG.debug('epoch time: {}'.format(epoch))

    masker_config = trans.MASKERS[args.masking]
    LOG.debug('using masker options {}'.format(masker_config))
    masker_config['salt'] = args.salt
    mask = trans.Masker(**masker_config)

    db = mysqlargs.connect(db=args.database)

    if args.loglevel <= logging.DEBUG:
        n_records = list(mysql.count_instances(db))[0]['Count(*)']
        LOG.debug('number of instance records: {}'.format(n_records))


    traces = mysql.traces(db, start=start, end=end)
    traces = pipeline(traces, 
        functools.partial(trans.mask_fields, trace_type=TRACE_TYPE, masker=mask),
        functools.partial(trans.extra_times, epoch=epoch),
    )

    #traces = itertools.islice(traces, 20)
    if args.jsons:
        LOG.debug('writing JSONs to {}'.format(args.output_file))
        jsons.write(args.output_file, traces, TRACE_TYPE)
    else:
        LOG.debug('writing CSV to {}'.format(args.output_file))
        csv.write(args.output_file, traces, TRACE_TYPE)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
