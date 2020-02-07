"""
Trying to use the peewee ORM, but it's giving slow performance with the
large queries, plus I can't figure out how to do a query with a couple
JOINs and get data from multiple tables at once...
"""
import argparse
import sys

from dateutil.parser import parse as dateparse
import peewee as pw

from ..masker import Masker
import ._nova_models as nm


def count_instances():
    '''connection tester'''
    # sq = pw.SelectQuery(nm.Instances)
    sq = nm.Instances.select()
    print(sq.count())

    active = sq.where(nm.Instances.deleted == '0')
    print(active.count())


def traces(*, start=None, end=None):
    query = (nm.Instances.select()
        .join(nm.InstanceActions)
        .join(nm.InstanceActionsEvents)
        .order_by(nm.InstanceActionsEvents.start_time)
        # .paginate(1, 10)
    )

    if start is not None:
        query = query.where(nm.InstanceActionsEvents.start_time >= start)
    if end is not None:
        query = query.where(nm.InstanceActionsEvents.start_time <= end)

    # print(query.sql())
    count = query.count()
    print(count)
    # pw.prefetch(
    #     nm.Instances.select(),
    #     nm.InstanceActions.select(),
    #     nm.InstanceActionsEvents.select(),
    # )
    PAGE_SIZE = 200
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument('-H', '--host', type=str, default='127.0.0.1',
        help='MySQL host')
    parser.add_argument('-p', '--port', type=int, default=3306,
        help='MySQL port')
    parser.add_argument('-u', '--user', type=str, default='readonly',
        help='MySQL user')

    parser.add_argument('-s', '--start', type=str)
    parser.add_argument('-e', '--end', type=str)

    args = parser.parse_args()

    start = dateparse(args.start) if args.start else None
    end = dateparse(args.end) if args.end else None

    connect_kwargs = {
        'host': args.host,
        'port': args.port,
        'user': args.user,
    }

    db = nm.database.init('nova', **connect_kwargs)
    mask = Masker('sha512', 32)

    # count_instances()
    for n, trace in enumerate(traces(start=start, end=end)):
        for field in ['user_id', 'project_id', 'hostname', 'host']:
            trace[field] = mask(trace[field])
        print(trace)
        if n > 10: break
