# coding: utf-8
import argparse
import datetime
import functools
import logging
import sys

import configparser

from dateutil.parser import parse as dateparse

from . import transforms as trans
from .extractors import mysql
from .formatters import csv_formatter, jsons
from .util import pipeline

TRACE_TYPE = 'instance'

LOG = logging.getLogger(__name__)

def datetime_serializer(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("don't know how to serialize object {}".format(repr(obj)))


def main(argv):
    config = configparser.ConfigParser()
    config.read('starcompactor.config')
    
    parser = argparse.ArgumentParser(description=__doc__)
    mysqlargs = mysql.MySqlArgs({
        'user': 'root',
        'password': '',
        'host': 'localhost',
        'port': 3306,
    })
    mysqlargs.inject(parser)
    parser.add_argument('--start', type=str)
    parser.add_argument('--end', type=str)
    parser.add_argument('--hashed-masking-method', type=str, default='sha2-salted', choices=trans.MASKERS,
        help='User data mask type. "sha1-raw" is legacy and not recommended as it is vulnerable to cracking. "none" is for debugging only.')
    parser.add_argument('--hashed-masking-salt', type=str, default=None,
        help = 'Salt of hashed masking method. Please use the same salt for machine event host name! Ignored if masking method is "none".')
    parser.add_argument('--instance-type', type=str, default='vm', choices=['vm', 'baremetal'],
        help='Type of the instance. Choose vm or baremetal')
    parser.add_argument('--jsons', action='store_true',
        help='Format output as one JSON per line (defaults to CSV-style)')
    parser.add_argument('--verbose', action='store_const', const=logging.INFO, dest="loglevel",
        help='Increase verbosity about the dump.')
    parser.add_argument('--debug', action='store_const', const=logging.DEBUG, dest="loglevel",
        help='Debug-level logging.')
    parser.add_argument('output_file', type=str, help='File to dump results')

    args = parser.parse_args(argv[1:])
    mysqlargs.extract(args)
    
    LOG.debug('instance type: {}'.format(args.instance_type))

    start = dateparse(args.start) if args.start else None
    end = dateparse(args.end) if args.end else None

    args = parser.parse_args()
    if args.loglevel is None:
        args.loglevel = logging.WARNING
    logging.basicConfig(level=args.loglevel)

    epoch = dateparse(config.get('default', 'epoch'))
    LOG.debug('epoch time: {}'.format(epoch))

    masker_config = trans.MASKERS[args.hashed_masking_method]
    LOG.debug('using masker options {}'.format(masker_config))
    masker_config['salt'] = args.hashed_masking_salt
    mask = trans.Masker(**masker_config)

    db = mysqlargs.connect()
    databases = config.get('default', 'nova_databases').split(',')

    if args.loglevel <= logging.DEBUG:
        n_records = 0
        for database in databases:
            n_records = n_records + int(list(mysql.count_instances(db, database))[0]['cnt'])
        LOG.debug('number of instance records: {}'.format(str(n_records)))

    traces = []
    for database in databases:
        t = mysql.traces(db, database, start=start, end=end)
        t = pipeline(t, 
                    functools.partial(trans.mask_fields, trace_type=TRACE_TYPE, masker=mask),
                    functools.partial(trans.extra_times, epoch=epoch),
                    )
        traces = traces + list(t)
        
    sorted(traces, key = lambda i: i['START_TIME'])  

    if args.jsons:
        LOG.debug('writing JSONs to {}'.format(args.output_file))
        jsons.write(args.output_file, traces, TRACE_TYPE, args.instance_type)
    else:
        LOG.debug('writing CSV to {}'.format(args.output_file))
        csv_formatter.write(args.output_file, traces, TRACE_TYPE, args.instance_type)

if __name__ == '__main__':
    sys.exit(main(sys.argv))
