# coding: utf-8
import logging

from dateutil.parser import parse as _dateparse

LOG = logging.getLogger(__name__)
OS_ENV_PREFIX = 'OS_'
PAGE_SIZE = 25

def dateparse(value):
    if value is None:
        return None
    return _dateparse(value)


def _paginate_servers(auth, **kwargs):
    '''
    Manually paginate servers(). When a page fetch fails because the marker
    instance is permanently broken (e.g. Nova 500 InstanceNotFound), skip
    that marker and try the next UUID from the previous page.  If every
    candidate in the page is broken, stop pagination for this query.
    '''
    marker = None
    last_page_ids = []

    while True:
        params = dict(kwargs, limit=PAGE_SIZE)
        if marker:
            params['marker'] = marker

        try:
            page = list(auth.compute.servers(**params))
        except Exception as e:
            LOG.warning(f'Failed to fetch page at marker {marker}: {e}')

            # Walk forward through the previous page's UUIDs to find one that works
            try:
                start = last_page_ids.index(marker) + 1
            except ValueError:
                LOG.error('Cannot advance past broken marker %s; stopping.', marker)
                return

            advanced = False
            for candidate in last_page_ids[start:]:
                try:
                    page = list(auth.compute.servers(**dict(params, marker=candidate)))
                    LOG.warning('Skipped broken marker %s, resumed at %s', marker, candidate)
                    marker = candidate
                    advanced = True
                    break
                except Exception:
                    continue

            if not advanced:
                LOG.error('All candidates after broken marker %s failed; stopping.', marker)
                return

        if not page:
            break

        last_page_ids = [s.id for s in page]
        yield from page

        if len(page) < PAGE_SIZE:
            break

        marker = last_page_ids[-1]


def all_instances(auth, include_deleted=True):
    '''
    Iterate over all instances ever.
    '''
    if include_deleted:
        yield from _paginate_servers(auth, all_projects=True, deleted=1)
        yield from _paginate_servers(auth, all_projects=True, deleted=0)
    else:
        yield from _paginate_servers(auth, all_projects=True, deleted=0)


def instance_actions(auth, server_id):
    '''
    Get the rough list of actions. As per the API reference: action details
    of deleted instances can be returned for requests later than
    microversion 2.21.
    '''
    return list(auth.compute.server_actions(server_id))


def instance_action_details(auth, server_id, request_id):
    '''
    Get details for the action.
    '''
    response = auth.compute.get(
        f'/servers/{server_id}/os-instance-actions/{request_id}',
        microversion='2.21',
    )
    return response.json()['instanceAction']


def traces_raw(auth):
    '''Yield instance/action/event data in all combinations.'''
    instances = list(all_instances(auth))
    LOG.info('Starting trace extraction...')

    for count, instance in enumerate(instances):
        if count % 25 == 0 and count > 0:
            LOG.info(f'Processed {count} instances...')

        for attempt in range(3):
            try:
                actions = instance_actions(auth, instance.id)
                break
            except Exception as e:
                LOG.warning(f'Attempt {attempt + 1}/3 failed fetching actions for {instance.id}: {e}')
        else:
            LOG.error(f'Skipping instance {instance.id} after 3 failed attempts')
            continue

        if not actions:
            LOG.info('instance {} has no actions'.format(instance.id))
            continue

        for action in actions:
            for attempt in range(3):
                try:
                    details = instance_action_details(auth, instance.id, action.request_id)
                    break
                except Exception as e:
                    LOG.warning(f'Attempt {attempt + 1}/3 failed fetching action details for {instance.id}/{action.request_id}: {e}')
            else:
                LOG.error(f'Skipping action {action.request_id} for instance {instance.id} after 3 failed attempts')
                continue

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
