from __future__ import absolute_import, division, print_function, unicode_literals
import argparse
import datetime
import json
import sys

from dateutil.parser import parse as dateparse

from .transforms import masker
from .extractors import mysql
from .formatters import csv


def datetime_serializer(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("don't know how to serialize object {}".format(repr(obj)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mysqlargs = mysql.MySqlArgs({
        'user': 'root',
        'password': '',
        'host': 'localhost',
        'port': 3306,
    })
    mysqlargs.inject(parser)
    parser.add_argument('-s', '--start', type=str)
    parser.add_argument('-e', '--end', type=str)
    parser.add_argument('-c', '--epoch', type=str, default='2015-09-06T23:31:16',
        help='Time to compute relative dates from')
    parser.add_argument('-m', '--masking', type=str, default='sha2-salted',
        choices=masker.MASKERS,
        help='User data mask type. "sha1-raw" is legacy and not recommended as '
             'it is vulnerable to cracking. "none" is for debugging only.')
    parser.add_argument('-j', '--jsons', action='store_true',
        help='Format output as one JSON per line (defaults to CSV-style)')
    parser.add_argument('output_file', type=str,
        help='File to dump results')

    args = parser.parse_args()
    mysqlargs.extract(args)

    start = dateparse(args.start) if args.start else None
    end = dateparse(args.end) if args.end else None
    epoch = dateparse(args.epoch)

    mask = masker.Masker(**masker.MASKERS[args.masking])

    db = mysqlargs.connect(db='nova')

    # print(list(count_instances(db)))

    with open(args.output_file, 'w') as f:
        if not args.jsons:
            f.write(','.join(csv._CSV_HEADER) + '\n')

        for n, trace in enumerate(mysql.traces_query(db, start=start, end=end)):
            print(trace)

            for field in ['user_id', 'project_id', 'hostname', 'host']:
                # if trace[field]:
                trace[field] = mask(trace[field])
                # else:
                    # trace[field] = ''

            trace['start_sec'] = (trace['start_time'] - epoch).total_seconds()
            if trace['finish_time'] is not None:
                trace['finish_sec'] = (trace['finish_time'] - epoch).total_seconds()
                trace['duration'] = (trace['finish_time'] - trace['start_time']).total_seconds()
            else:
                trace['finish_sec'] = None
                trace['duration'] = None

            if args.jsons:
                f.write(json.dumps(trace, default=datetime_serializer) + '\n')
            else:
                f.write(csv.csv_row(trace) + '\n')

            if n > 10: break
