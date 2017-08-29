# coding: utf-8
from __future__ import absolute_import, print_function, unicode_literals

import datetime
import functools
import logging
import os

from dateutil.parser import parse as dateparse
from dateutil.tz import tzutc
import requests


OS_ENV_PREFIX = 'OS_'
PAGE_SIZE = 25


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
        self.authenticate()

    def authenticate(self):
        response = requests.post(self.rc['OS_AUTH_URL'] + '/tokens', json={
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

        return service['endpoints'][0]['publicURL']


def _instances(auth, limit=100, marker=None, deleted=False):
    '''Get one page of instances'''
    params = {
        'all_tenants': True,
        'deleted': deleted,
    }
    if limit:
        params['limit'] = limit
    if marker:
        params['marker'] = marker

    print('GET /servers/detail')
    response = requests.get(
        auth.endpoint('compute') + '/servers/detail',
        headers={
            'X-Auth-Token': auth.token,
        },
        params=params,
    )
    response.raise_for_status()
    return response.json()['servers']


def all_instances(auth, _pagesize=PAGE_SIZE):
    '''Iterate over all instances ever.'''
    # for deleted_state in [True, False]:
    for deleted_state in [False]:
        _instancesp = functools.partial(_instances, auth, limit=_pagesize, deleted=deleted_state)
        insts = _instancesp()
        while True:
            for inst in insts:
                print(inst)
                yield inst
            insts = _instancesp(marker=inst['id'])
            if len(insts) == 0:
                break


def instance_actions(auth, server_id):
    print('GET /servers/<id>/os-instance-actions')
    response = requests.get(
        auth.endpoint('compute') + '/servers/{}/os-instance-actions'.format(server_id),
        headers={
            'X-Auth-Token': auth.token,
            # must specify API microversion to see a deleted server's actions
            # 'X-OpenStack-Nova-API-Version': '2.21',
        },
    )
    response.raise_for_status()
    return response.json()['instanceActions']


def instance_action_details(auth, server_id, request_id):
    print('GET /servers/<id>/os-instance-actions/<request>')
    response = requests.get(
        auth.endpoint('compute') + '/servers/{}/os-instance-actions/{}'.format(server_id, request_id),
        headers={
            'X-Auth-Token': auth.token,
            # 'X-OpenStack-Nova-API-Version': '2.21',
        },
    )
    response.raise_for_status()
    return response.json()['instanceAction']


_flavor_cache = {}
#@functools.lru_cache() # Py3.2+
def nova_flavor(auth, flavor_id):
    # <py2 memoize hack>
    global _flavor_cache
    try:
        return _flavor_cache[flavor_id]
    except KeyError:
        pass
    # </py2 memoize hack>
    print('GET /flavors/<id>')
    response = requests.get(
        auth.endpoint('compute') + f'/flavors/{flavor_id}',
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
    for instance in all_instances(auth):
        actions = instance_actions(auth, instance['id'])
        for action in actions:
            details = instance_action_details(auth, instance['id'], action['request_id'])
            for event in details['events']:
                yield instance, action, event


def traces(auth):
    for instance, action, event in traces_raw(auth):
        flavor = nova_flavor(auth, instance['flavor']['id'])
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
