# coding: utf-8
from __future__ import absolute_import, print_function, unicode_literals

import functools
import logging

from dateutil.parser import parse as _dateparse
import requests

LOG = logging.getLogger(__name__)
OS_ENV_PREFIX = 'OS_'
PAGE_SIZE = 25

def dateparse(value):
    if value is None:
        return None
    return _dateparse(value)

def _instances(auth, limit=100, marker=None, deleted=False):
    '''
    Get one page of instances

    .. seealso::

        `OpenStack Compute API Reference <https://developer.openstack.org/api-ref/compute/#list-servers-detailed>`_
    '''
    params = {
        'all_tenants': True,
    }
    if deleted:
        # If you send "deleted=False" or "deleted=0" to the API, it
        # interprets that as True.
        params['deleted'] = 1
    if limit:
        params['limit'] = limit
    if marker:
        params['marker'] = marker

    LOG.info('GET /servers/detail, params: {}'.format(params))
    response = requests.get(
        auth.endpoint('compute') + '/servers/detail',
        headers={
            'X-Auth-Token': auth.token,
        },
        params=params,
    )
    response.raise_for_status()
    return response.json()['servers']


def all_instances(auth, include_deleted=True, _pagesize=PAGE_SIZE):
    '''
    Iterate over all instances ever. Only requests a page (*_pagesize*)
    at a time to avoid large/unbounded memory use and possible problems with
    the HTTP API.
    '''
    # Deleted/non-deleted instances are iterated over separately because
    # "deleted = True" means show *only* deleted instances, not *both* deleted
    # and undeleted instances.
    if include_deleted:
        deleted_states = [True, False]
    else:
        deleted_states = [False]

    for deleted_state in deleted_states:
        _instancesp = functools.partial(_instances, auth, limit=_pagesize, deleted=deleted_state)
        insts = _instancesp()
        while True:
            for inst in insts:
                LOG.debug(inst)
                yield inst
            insts = _instancesp(marker=inst['id'])
            if len(insts) == 0:
                break

def instance_actions(auth, server_id):
    '''
    Get the rough list of actions. As per the API reference: action details
    of deleted instances can be returned for requests later than
    microversion 2.21.

    .. seealso::

        `OpenStack Compute API Reference <https://developer.openstack.org/api-ref/compute/#list-actions-for-server>`_
    '''
    LOG.info('GET /servers/{}/os-instance-actions'.format(server_id))
    response = requests.get(
        auth.endpoint('compute') + '/servers/{}/os-instance-actions'.format(server_id),
        headers={
            'X-Auth-Token': auth.token,
            # must specify API microversion to see a deleted server's actions
            'X-OpenStack-Nova-API-Version': '2.21',
        },
    )
    response.raise_for_status()
    return response.json()['instanceActions']


def instance_action_details(auth, server_id, request_id):
    '''
    Get details for the action. This is required to split up an action into
    individual events (like scheduling vs. executing?), and also for the
    following field data:

    * finish_time
    * result (does this correlate with message that the actions list already has?)

    .. seealso::

        `OpenStack Compute API Reference <https://developer.openstack.org/api-ref/compute/#show-server-action-details>`_
    '''
    LOG.info('GET /servers/{}/os-instance-actions/{}'.format(server_id, request_id))
    response = requests.get(
        auth.endpoint('compute') + '/servers/{}/os-instance-actions/{}'.format(server_id, request_id),
        headers={
            'X-Auth-Token': auth.token,
            'X-OpenStack-Nova-API-Version': '2.21',
        },
    )
    response.raise_for_status()
    return response.json()['instanceAction']


_flavor_cache = {}
#@functools.lru_cache() # Py3.2+
def nova_flavor(auth, flavor_id):
    '''
    Gets information about the Nova flavor so it can be attached to the
    trace event row.

    .. note::

        All events for a given instance will always have the same
        CPUs/RAM/disk.

    Newer (2.47?) Nova APIs will do this little request internally when
    getting the instance details. By memoizing this, it should drastically
    reduce the overall number of requests. There's no bound on the cache,
    but hopefully the number of flavors is less than 1000.
    '''
    # <py2 memoize hack>
    global _flavor_cache
    try:
        return _flavor_cache[flavor_id]
    except KeyError:
        pass
    # </py2 memoize hack>
    LOG.info('GET /flavors/{}'.format(flavor_id))
    response = requests.get(
        auth.endpoint('compute') + '/flavors/{}'.format(flavor_id),
        headers={
            'X-Auth-Token': auth.token,
            'X-OpenStack-Nova-API-Version': '2.21',
        },
    )
    response.raise_for_status()

    flavor = response.json()['flavor']
    # <py2 memoize hack>
    _flavor_cache[flavor_id] = flavor
    # </py2 memoize hack>
    return flavor


def traces_raw(auth):
    '''Yield instance/action/event data in all combinations.'''
    for instance in all_instances(auth):
        actions = instance_actions(auth, instance['id'])
        if not actions:
            LOG.info('instance {} has no actions'.format(instance['id']))
            continue

        for action in actions:
            details = instance_action_details(auth, instance['id'], action['request_id'])
            for event in details['events']:
                yield instance, action, event


def traces(auth):
    '''Extract the desired fields from the instance/action/event combos.'''
    for instance, action, event in traces_raw(auth):
        flavor = nova_flavor(auth, instance['flavor']['id'])
        if event['finish_time'] is None:
            LOG.debug('Invalid event %s', event)
        else:
            yield {
                'INSTANCE_UUID': instance['id'],
                'vcpus': flavor['vcpus'],
                'memory_mb': flavor['ram'],
                'root_gb': flavor['disk'],
                'USER_ID': instance['user_id'],
                'PROJECT_ID': instance['tenant_id'],
                'INSTANCE_NAME': instance['name'],
                'HOST_NAME (PHYSICAL)': instance['OS-EXT-SRV-ATTR:host'],
                'EVENT': event['event'],
                'RESULT': event['result'],
                'START_TIME': dateparse(event['start_time']),
                'FINISH_TIME': dateparse(event['finish_time']),
            }
