# coding: utf-8
import functools
import logging

from dateutil.parser import parse as _dateparse

LOG = logging.getLogger(__name__)
OS_ENV_PREFIX = 'OS_'
PAGE_SIZE = 25

def dateparse(value):
    if value is None:
        return None
    return _dateparse(value)

def all_instances(auth, include_deleted=True):
    '''
    Iterate over all instances ever.
    '''
    if include_deleted:
        yield from auth.compute.servers(all_projects=True, deleted=1)
        yield from auth.compute.servers(all_projects=True, deleted=0)
    else:
        yield from auth.compute.servers(all_projects=True, deleted=0)

def instance_actions(auth, server_id):
    '''
    Get the rough list of actions. As per the API reference: action details
    of deleted instances can be returned for requests later than
    microversion 2.21.
    '''
    # LOG.info(f'Fetching actions for server {server_id}')
    return list(auth.compute.server_actions(server_id))


def instance_action_details(auth, server_id, request_id):
    '''
    Get details for the action.
    '''
    # The compute SDK doesn't natively expose action detail fetch in generator form 
    # as easily, but we can call it explicitly using the internal SDK method if it exists,
    # or fallback to `.get()` 
    response = auth.compute.get(
        f'/servers/{server_id}/os-instance-actions/{request_id}',
        microversion='2.21',
    )
    return response.json()['instanceAction']


def traces_raw(auth):
    '''Yield instance/action/event data in all combinations.'''
    instances = all_instances(auth)
    LOG.info('Starting trace extraction...')

    for count, instance in enumerate(instances):
        if count % 25 == 0 and count > 0:
            LOG.info(f'Processed {count} instances...')

        actions = instance_actions(auth, instance.id)
        if not actions:
            LOG.info('instance {} has no actions'.format(instance.id))
            continue

        for action in actions:
            details = instance_action_details(auth, instance.id, action.request_id)
            for event in details['events']:
                yield instance, action, event


def traces(auth):
    '''Extract the desired fields from the instance/action/event combos.'''
    for instance, action, event in traces_raw(auth):
        if event['finish_time'] is None:
            LOG.debug('Invalid event %s', event)
        else:
            yield {
                'INSTANCE_UUID': instance.id,
                'vcpus': instance.flavor.vcpus,
                'memory_mb': instance.flavor.ram,
                'root_gb': instance.flavor.disk,
                'USER_ID': instance.user_id,
                'PROJECT_ID': instance.project_id,
                'INSTANCE_NAME': instance.name,
                'HOST_NAME (PHYSICAL)': instance.hypervisor_hostname,
                'EVENT': event['event'],
                'RESULT': event['result'],
                'START_TIME': dateparse(event['start_time']),
                'FINISH_TIME': dateparse(event['finish_time']),
            }
