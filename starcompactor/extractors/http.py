# coding: utf-8
from __future__ import absolute_import, print_function, unicode_literals

import datetime
import functools
import logging
import os
import re

from dateutil.parser import parse as _dateparse
from dateutil.tz import tzutc
import requests

LOG = logging.getLogger(__name__)
OS_ENV_PREFIX = 'OS_'
PAGE_SIZE = 25


def dateparse(value):
    if value is None:
        return None
    return _dateparse(value)


def find_v2_endpoint(auth_url):
    """
    Because sometimes OS_AUTH_URL points at one version or all the
    versions... #bad-ad-hoc-driven-coding #BADcoding
    """
    response = requests.get(auth_url)
    response.raise_for_status()
    data = response.json()
    if 'versions' in data:
        ids = []
        for version in data['versions']['values']:
            id_ = version['id']
            if id_ == 'v2.0':
                for link in version['links']:
                    if link['rel'] == 'self':
                        return link['href']
                else:
                    id_ += ' (no self-link provided)'
            ids.append(id_)
        raise RuntimeError('Could not find "v2.0" auth endpoint (found {})'.format(ids))

    elif 'version' in data:
        id_ = data['version']['id']
        if id_ == 'v2.0':
            return auth_url
        # try to remove the last part?
        raise RuntimeError('Auth endpoint not "v2.0" (is "{}")'.format(id_))
    else:
        # blindly hope it'll work?
        return auth_url


def load_osrc(fn, get_pass=False):
    '''Parse a Bash RC file dumped out by the dashboard to a dict.
    Used to load the file specified by :py:func:`add_arguments`.'''
    envval = re.compile(r'''
        \s* # maybe whitespace
        (?P<key>[A-Za-z0-9_\-$]+)  # variable name
        =
        ([\'\"]?)                  # optional quote
        (?P<value>.*)              # variable content
        \2                         # matching quote
        ''', flags=re.VERBOSE)
    rc = {}
    with open(fn, 'r') as f:
        for line in f:
            match = envval.search(line)
            if not match:
                continue
            match = match.groupdict()
            rc[match['key']] = match['value']

    try:
        password = rc['OS_PASSWORD']
    except KeyError:
        pass
    else:
        if password == '$OS_PASSWORD_INPUT':
            rc.pop('OS_PASSWORD')

    if get_pass:
        rc['OS_PASSWORD'] = getpass.getpass('Enter your password: ')

    return rc


# from https://github.com/ChameleonCloud/hammers/blob/master/hammers/osapi.py
class Auth(object):
    L = logging.getLogger(__name__ + '.Auth')

    required_os_vars = {
        'OS_USERNAME',
        'OS_PASSWORD',
        'OS_TENANT_NAME',
        'OS_AUTH_URL',
    }

    @classmethod
    #def from_env_or_args(cls, *, args=None, env=True): # Py3.x
    def from_env_or_args(cls, **kwargs):
        """
        Combine the provided *args* (if provided) with the environment vars
        (if *env*, default true) and produce an Auth object for use by REST
        functions.
        """
        # <py2 hack>
        args = kwargs.get('args', None)
        env = kwargs.get('env', True)
        # </py2 hack>
        os_vars = {}
        if env:
            os_vars = {k: os.environ[k] for k in os.environ if k.startswith(OS_ENV_PREFIX)}
        if args and args.osrc:
            os_vars.update(load_osrc(args.osrc))
        return cls(os_vars)

    def __init__(self, rc):
        self.rc = rc
        missing_vars = self.required_os_vars - set(rc)
        if missing_vars:
            raise RuntimeError('Missing required OS values: {}'.format(missing_vars))

        self.region = self.rc.get('OS_REGION', None)
        self.L.debug('region = "{}"'.format(self.region))
        self.v2endpoint = find_v2_endpoint(self.rc['OS_AUTH_URL'])
        self.L.debug('auth endpoint = "{}"'.format(self.v2endpoint))
        self.authenticate()

    def authenticate(self):
        self.L.debug('authenticating')
        response = requests.post(self.v2endpoint + '/tokens', json={
        'auth': {
            'passwordCredentials': {
                'username': self.rc['OS_USERNAME'],
                'password': self.rc['OS_PASSWORD'],
            },
            'tenantName': self.rc['OS_TENANT_NAME']
        }})
        if response.status_code != 200:
            raise RuntimeError(
                'HTTP {}: {}'
                .format(response.status_code, response.content[:400])
            )

        jresponse = response.json()
        try:
            self.access = jresponse['access']
        except KeyError:
            raise RuntimeError(
                'expected "access" key not present in response '
                '(found keys: {})'.format(list(jresponse))
            )

        self._token = self.access['token']['id']
        self.expiry = dateparse(self.access['token']['expires'])

        self.L.debug('New token "{}" expires in {:.2f} minutes'.format(
            self._token,
            (self.expiry - datetime.datetime.now(tz=tzutc())).total_seconds() / 60
        ))

    @property
    def token(self):
        if (self.expiry - datetime.datetime.now(tz=tzutc())).total_seconds() < 60:
            self.authenticate()

        return self._token

    def endpoint(self, type):
        services = [
            service
            for service
            in self.access['serviceCatalog']
            if service['type'] == type
        ]
        if len(services) < 1:
            raise RuntimeError("didn't find any services matching type '{}'".format(type))
        elif len(services) > 1:
            raise RuntimeError("found multiple services matching type '{}'".format(type))
        service = services[0]
        endpoints = service['endpoints']

        if self.region:
            for endpoint in endpoints:
                if endpoint['region'] == self.region:
                    # leak endpoint out of the loop
                    break
            else:
                raise RuntimeError("didn't find any endpoints for region '{}'".format(self.region))
        else:
            # pick arbitrary region if none provided.
            endpoint = endpoints[0]

        return endpoint['publicURL']


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
                'uuid': instance['id'],
                'vcpus': flavor['vcpus'],
                'memory_mb': flavor['ram'],
                'root_gb': flavor['disk'],
                'user_id': instance['user_id'],
                'project_id': instance['tenant_id'],
                'hostname': instance['name'],
                'host': instance['OS-EXT-SRV-ATTR:host'],
                'event': event['event'],
                'result': event['result'],
                'start_time': dateparse(event['start_time']),
                'finish_time': dateparse(event['finish_time']),
            }
