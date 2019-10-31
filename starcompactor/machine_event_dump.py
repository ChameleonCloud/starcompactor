# coding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals
import argparse
import contextlib
import logging
import math
import multiprocessing
import os
import re
import sys
import traceback

try:
    import configparser # 3.x
except ImportError:
    from backports import configparser # 2.x 3rd party

from dateutil.parser import parse as dateparse
from os import listdir
from os.path import isfile, join

from . import transforms as trans
from .extractors import machine, mysql
from .formatters import csv, jsons

TRACE_TYPE = 'machine'

LOG = logging.getLogger(__name__)

def refine_machine_events(machine_events):
    # sort machine event keys (event_time, host, event_type) by host and event_time
    sorted_key = sorted(machine_events.keys(), key=lambda x: (x[1], x[0], x[2]))
    current_host = None
    prev_event = None
    prev_properties = None
    is_created = False
    is_enabled = False
    for key in sorted_key:
        event_time = key[0]
        host = key[1]
        event_type = key[2]
        if host != current_host:
            current_host = host
            prev_event = None
            prev_properties = None
            is_created = False
            is_enabled = False
        if not prev_event:
            # first event of a host must be CREATE
            if event_type != 'CREATE':
                del machine_events[key]
                continue
        else:
            # consecutive events with different timestamp but same properties and same event type is invalid; take the earlist timestamp
            if event_type == prev_event and machine_events[key] == prev_properties:
                del machine_events[key]
                continue
            # the UPDATE event after any event must have different properties
            if event_type == 'UPDATE' and machine_events[key] == prev_properties:
                del machine_events[key]
                continue
            # valid open/close pairs
            if event_type == 'CREATE' and is_created:
                del machine_events[key]
                continue
            if event_type == 'DELETE' and not is_created:
                del machine_events[key]
                continue
            if event_type == 'ENABLE' and is_enabled:
                del machine_events[key]
                continue
            if event_type == 'DISABLE' and not is_enabled:
                del machine_events[key]
                continue
        prev_event = event_type
        prev_properties = machine_events[key]
        if event_type == 'CREATE': is_created = True
        if event_type == 'DELETE': is_created = False
        if event_type == 'ENABLE': is_enabled = True
        if event_type == 'DISABLE': is_enabled = False
    
    return machine_events

def mask_and_derive(machine_events, masker, epoch, rack_property_name):
    traces = []
    for key in machine_events.keys():
        trace = machine_events[key].copy()
        trace['EVENT_TIME'] = key[0]
        trace['HOST_NAME (PHYSICAL)'] = key[1]
        trace['EVENT'] = key[2]
        trace = trans.mask_fields(trace, TRACE_TYPE, masker)
        trace = trans.machine_event_times(trace, epoch)
        if rack_property_name:
            trace = trans.ordered_mask(trace, rack_property_name, sorted(set([machine_events[k][rack_property_name] for k in machine_events.keys() if rack_property_name in machine_events[k]]))) 
        traces.append(trace) 
    
    return traces

def get_machine_event_with_packed_args(args):
    try:
        return machine.get_machine_event(**args)
    except Exception as e:
        traceback.print_exc()
        raise e
    
def main(argv):
    config = configparser.ConfigParser()
    config.read('starcompactor.config')
    
    parser = argparse.ArgumentParser(description=__doc__)
    mysqlargs = mysql.MySqlArgs({
        'user': 'root',
        'password': '',
        'host': 'localhost',
        'port': 3306,
    })
    mysqlargs.inject(parser)
    parser.add_argument('--hashed-masking-method', type=str, default='sha2-salted', choices=trans.MASKERS,
        help='Hashed mask method (for host name). "sha1-raw" is legacy and not recommended as it is vulnerable to cracking. "none" is for debugging only.')
    parser.add_argument('--hashed-masking-salt', type=str, default=None,
        help = 'Salt of hashed masking method (for host name). Please use the same salt for instance event host name! Ignored if host name mask type is "none".')
    parser.add_argument('--instance-type', type=str, default='vm', choices=['vm', 'baremetal'],
        help='Type of the instance. Choose vm or baremetal')
    parser.add_argument('--jsons', action='store_true',
        help='Format output as one JSON per line (defaults to CSV-style)')
    parser.add_argument('--verbose', action='store_const', const=logging.INFO, dest="loglevel",
        help='Increase verbosity about the dump.')
    parser.add_argument('--debug', action='store_const', const=logging.DEBUG, dest="loglevel",
        help='Debug-level logging.')
    parser.add_argument('output_file', type=str,
        help='File to dump results')

    args = parser.parse_args(argv[1:])
    mysqlargs.extract(args)

    if args.loglevel is None:
        args.loglevel = logging.WARNING
    logging.basicConfig(level=args.loglevel)

    LOG.debug('instance type: {}'.format(args.instance_type))

    epoch = dateparse(config.get('default', 'epoch'))
    LOG.debug('epoch time: {}'.format(epoch))

    # mask for host name
    masker_config = trans.MASKERS[args.hashed_masking_method]
    LOG.debug('using masker options {}'.format(masker_config))
    masker_config['salt'] = args.hashed_masking_salt
    
    mask = trans.Masker(**masker_config)

    backup_dir = config.get('backup', 'backup_dir')
    backup_files = [join(backup_dir, f) for f in listdir(backup_dir) if isfile(join(backup_dir, f)) and re.match(config.get('backup', 'backup_file_regex'), f)]
    backup_files.sort(key=lambda x: os.path.getmtime(x))
        
    machine_args = []
    process_no = 0
    for backup_file in backup_files:
        arg = {'process_no': process_no,
               'backup_file': backup_file, 
               'mysql_args': mysqlargs.connect_kwargs, 
               'instance_type': args.instance_type}
        machine_args.append(arg)  
        process_no = process_no + 1
    
    with contextlib.closing(multiprocessing.Pool(processes=int(math.ceil(process_no / int(config.get('multithread', 'number_of_files_per_process')))))) as pool:
        process_results = pool.map(get_machine_event_with_packed_args, machine_args)
    
    machine_events = {}
    # multiprocessing keeps the order
    # create DELETE events
    prev_hosts = None
    process_no = 0
    for result in process_results:
        if not result or 'hosts' not in result:
            LOG.warn('missing result from process {}'.format(str(process_no)))
            process_no = process_no + 1
            continue
        current_hosts = result['hosts']
        if prev_hosts:
            deleted_hosts = prev_hosts.difference(current_hosts)
            for host in deleted_hosts:
                machine_events[(result['file_time'], host, 'DELETE')] = {}
        prev_hosts = current_hosts
        process_no = process_no + 1
    
    # we need to reverse the order for "CREATE" event
    # so that we get the contents for create events from the earliest backup file.
    # Example:
    # In 2015-10-09 backup and for host x, "created_at" 2015-10-09 with 48 available VCPUs.
    # In 2017-11-01 backup and for host x, "created_at" 2015-10-09 with 40 available VCPUs.
    # For create event of host x, we choose the content from 2015-10-09 backup.
    process_results.reverse()
    for result in process_results:
        if result and 'machine_events' in result:
            machine_events.update(result['machine_events'])
        
    # refine machine events
    # 1. consecutive events with different timestamps but the same event type and properties is not valid; pick the earliest timestamp
    # 2. the UPDATE event after any event must have different properties
    # 3. the first event of a host must be CREATE
    # 4. valid open/close pairs (CREATE and DELETE; ENABLE and DISABLE)
    refined_machine_events = refine_machine_events(machine_events)
    
    # mask and derive
    rack_property_name = (
        config.get('baremetal', 'rack_property_name')
        if args.instance_type == 'baremetal' else 'rack')
    traces = mask_and_derive(refined_machine_events, mask, epoch, rack_property_name)
       
    if args.jsons:
        LOG.debug('writing JSONs to {}'.format(args.output_file))
        jsons.write(args.output_file, traces, TRACE_TYPE, args.instance_type)
    else:
        LOG.debug('writing CSV to {}'.format(args.output_file))
        csv.write(args.output_file, traces, TRACE_TYPE, args.instance_type)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
