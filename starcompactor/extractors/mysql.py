from __future__ import absolute_import, division, print_function, unicode_literals
import argparse
import datetime
import json
import sys

from dateutil.parser import parse as dateparse


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


# def traces(db, start=None, end=None):
#     results = traces_query(db, start, end)

#     for page in range(count // PAGE_SIZE):
#         for instance in query.paginate(page, PAGE_SIZE).iterator():
#             ia = instance.instanceactions_set[0]
#             iae =  ia.instanceactionsevents_set[0]
#             event = {
#                 'uuid': instance.uuid,
#                 'vcpus': instance.vcpus,
#                 'memory_mb': instance.memory_mb,
#                 'root_gb': instance.root_gb,
#                 'user_id': instance.user,
#                 'project_id': instance.project,
#                 'hostname': instance.hostname,
#                 'host': instance.host,

#                 'event': iae.event,
#                 'result': iae.result,
#                 'start_time': iae.start_time,
#                 'finish_time': iae.finish_time,
#             }
#             yield event
