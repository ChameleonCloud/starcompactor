from __future__ import absolute_import, division, print_function, unicode_literals
import logging

from ._mysql import MyCnf, MySqlArgs, MySqlShim


LOG = logging.getLogger(__name__)

TRACE_EVENT_KEY_RENAME_MAP = {'uuid': 'INSTANCE_UUID',
                              'event': 'EVENT',
                              'start_time': 'START_TIME',
                              'finish_time': 'FINISH_TIME',
                              'result': 'RESULT',
                              'hostname': 'INSTANCE_NAME',
                              'user_id': 'USER_ID',
                              'project_id': 'PROJECT_ID',
                              'host': 'HOST_NAME (PHYSICAL)'}

def count_instances(db, database_name):
    '''
    Connection test
    '''
    sql = '''
    SELECT count(*) as cnt
    FROM {database_name}.instances;
    '''.format(database_name=database_name)
    return db.query(sql, limit=None)

def traces_query(db, database_name, start=None, end=None):
    # instances that belong to admin are excluded.
    # these instances were created before KVM site came alive for testing purposes.
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
        iae.event,
        iae.result,
        iae.start_time,
        iae.finish_time
    FROM
        {database_name}.instances AS i
            JOIN
        {database_name}.instance_actions AS ia ON i.uuid = ia.instance_uuid
            JOIN
        {database_name}.instance_actions_events AS iae ON ia.id = iae.action_id
    WHERE
        i.user_id != 'admin'
        AND
        i.project_id != 'admin'
    '''.format(database_name=database_name)

    conditionals = []
    params = []
    if start is not None:
        conditionals.append('ia.created_at >= %s')
        params.append(start)
    if end is not None:
        conditionals.append('ia.created_at <= %s')
        params.append(end)

    if conditionals:
        sql = '{} AND {}'.format(sql, ' AND '.join(conditionals))

    return db.query(sql, args=params, limit=None)

def traces(db, database, start=None, end=None):
    results = traces_query(db, database, start, end)

    for event in results:
        for key in event:
            if key in TRACE_EVENT_KEY_RENAME_MAP.keys(): event[TRACE_EVENT_KEY_RENAME_MAP[key]] = event.pop(key)
        if not event['FINISH_TIME']:
            LOG.debug('Invalid event %s', event)
            continue

        yield event
