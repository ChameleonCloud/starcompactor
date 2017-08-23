from __future__ import absolute_import, division, print_function, unicode_literals
import argparse
import datetime
import json
import sys

from dateutil.parser import parse as dateparse

from .masker import Masker
from . import mysql


def count_instances(db):
    '''
    Connection test
    '''
    sql = '''
    SELECT Count(*)
    FROM   instances
    WHERE  deleted='0';
    '''
    return db.query(sql, limit=None)


def datetime_serializer(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("don't know how to serialize object {}".format(repr(obj)))


def traces_query(db, start=None, end=None):
    sql = '''
    SELECT
        i.uuid,
        i.memory_mb,
        i.root_gb,
        i.vcpus,
        i.user_id,
        i.project_id,
        i.hostname,
        i.host,
        -- ia.created_at,
        -- ia.id AS action_id,
        -- iae.id AS event_id,
        iae.event,
        iae.result,
        iae.start_time,
        iae.finish_time
    FROM
        nova.instances AS i
            JOIN
        nova.instance_actions AS ia ON i.uuid = ia.instance_uuid
            JOIN
        nova.instance_actions_events AS iae ON ia.id = iae.action_id
    '''

    conditionals = []
    params = []
    if start is not None:
        conditionals.append('ia.created_at >= %s')
        params.append(start)
    if end is not None:
        conditionals.append('ia.created_at <= %s')
        params.append(end)

    if conditionals:
        sql = '{} WHERE {}'.format(sql, ' AND '.join(conditionals))

    sql += ' ORDER BY ia.created_at'
    return db.query(sql, args=params, limit=None)


def traces(db, start=None, end=None):
    results = traces_query(db, start, end)

    for page in range(count // PAGE_SIZE):
        for instance in query.paginate(page, PAGE_SIZE).iterator():
            ia = instance.instanceactions_set[0]
            iae =  ia.instanceactionsevents_set[0]
            event = {
                'uuid': instance.uuid,
                'vcpus': instance.vcpus,
                'memory_mb': instance.memory_mb,
                'root_gb': instance.root_gb,
                'user_id': instance.user,
                'project_id': instance.project,
                'hostname': instance.hostname,
                'host': instance.host,

                'event': iae.event,
                'result': iae.result,
                'start_time': iae.start_time,
                'finish_time': iae.finish_time,
            }
            # print(event)
            yield event
            # print(ia, iae)
            # import code;code.interact(local=locals())
            # return

FIELD_ORDER = ['']


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
    parser.add_argument('-m', '--no-masking', action='store_true',
        help='Don\'t mask user data')
    parser.add_argument('-j', '--jsons', action='store_true',
        help='Format output as one JSON per line (defaults to CSV-style)')
    parser.add_argument('output_file', type=str,
        help='File to dump results')

    args = parser.parse_args()
    mysqlargs.extract(args)

    start = dateparse(args.start) if args.start else None
    end = dateparse(args.end) if args.end else None
    epoch = dateparse(args.epoch)

    mask = Masker('sha512', 32)

    db = mysqlargs.connect(db='nova')

    # print(list(count_instances(db)))

    with open(args.output_file, 'w') as f:
        for n, trace in enumerate(traces_query(db, start=start, end=end)):
            if not args.no_masking:
                for field in ['user_id', 'project_id', 'hostname', 'host']:
                    trace[field] = mask(trace[field])

            trace['start_sec'] = (trace['start_time'] - epoch).total_seconds()
            if trace['finish_time'] is not None:
                trace['finish_sec'] = (trace['finish_time'] - epoch).total_seconds()
                trace['duration'] = (trace['finish_time'] - trace['start_time']).total_seconds()
            else:
                trace['finish_sec'] = None
                trace['duration'] = None

            f.write(json.dumps(trace, default=datetime_serializer) + '\n')
            # print(trace)
            # if n > 10: break
