# coding: utf-8
from __future__ import absolute_import, division, print_function
import bz2
import gzip
import csv
import logging
import os
import re
import shutil
import subprocess
import tempfile

from . import mysql

LOG = logging.getLogger(__name__)

NOVA_COMPUTE_HOST_BINARY = 'nova-compute'

def open_by_suffix(filename):
    if filename.endswith('.gz'):
        return gzip.open(filename, 'r')
    elif filename.endswith('.bz2'):
        return bz2.BZ2File(filename, 'r')
    else:
        return open(filename, 'r')

def get_machine_event(process_no, backup_file, mysql_args, rack_extractor):
    machine_events = {} # key is tuple (event_time, host_name, event)
    db = mysql.MySqlShim(**mysql_args)
    
    LOG.debug("Process {}: parsing file {}".format(str(process_no), backup_file))
    
    tmp_path = tempfile.mkdtemp()
    nova_tmp_database_name = 'kvm_nova_backup_{}'.format(os.path.basename(backup_file).split('.')[0]).replace('-', '_')
    tmp_sql_file_name = 'kvm-{}.sql'.format(os.path.basename(backup_file).split('.')[0])
    
    try:
        with open_by_suffix(backup_file) as f_in:
            filedata = f_in.read()
            with open(os.path.join(tmp_path, tmp_sql_file_name), 'w') as f_out:
                f_out.write(filedata)
    except:
        LOG.exception("Failed to read {}".format(backup_file))
        shutil.rmtree(tmp_path) 
        return machine_events
            
    db.cursor.execute('DROP DATABASE IF EXISTS {}'.format(nova_tmp_database_name))
    db.cursor.execute('CREATE DATABASE {}'.format(nova_tmp_database_name)) 
       
    for table in ['compute_nodes', 'services']: 
        tmp_table_sql_file_name = '{}_{}'.format(table, tmp_sql_file_name)
        process_extract_table = subprocess.Popen("sed -n -e '/DROP TABLE.*`{}`/,/UNLOCK TABLES/p' {} > {}".format(table, os.path.join(tmp_path, tmp_sql_file_name), os.path.join(tmp_path, tmp_table_sql_file_name)), shell=True)
        process_extract_table.wait()
        process_nova = subprocess.Popen('mysql --user={} --password={} --one-database {} < {}'.format(mysql_args['user'], mysql_args['passwd'], nova_tmp_database_name, os.path.join(tmp_path, tmp_table_sql_file_name)), shell=True)
        process_nova.wait()
    
    extract_data_sql = '''
                       SELECT cn.created_at AS created_at, cn.updated_at AS node_updated_at, 
                       cn.deleted_at AS deleted_at, s.updated_at AS service_updated_at,
                       s.disabled AS disabled, `binary`, hypervisor_hostname, vcpus, memory_mb, local_gb
                       FROM {database_name}.compute_nodes AS cn
                       LEFT JOIN {database_name}.services AS s
                       ON cn.host = s.host
                       '''.format(database_name = nova_tmp_database_name)
    try:
        db.cursor.execute(extract_data_sql)
        for row in db.cursor:
            create_time = row[0]
            node_update_time = row[1]
            delete_time = row[2]
            service_update_time = row[3]
            disabled = row[4]
            binary = row[5]
            hostname = row[6]
            vcpus = row[7]
            memory = row[8]
            disk = row[9]
            
            if not binary or binary != NOVA_COMPUTE_HOST_BINARY:
                continue
            rack = 'UNKNOWN'
            if rack_extractor['hypervisor_hostname_regex'] and rack_extractor['rack_extract_group']:
                m = re.search(rack_extractor['hypervisor_hostname_regex'], hostname)
                if m:
                    rack = m.group(rack_extractor['rack_extract_group'])
            content = {'RACK': rack,
                       'VCPU_CAPACITY': vcpus,
                       'MEMORY_CAPACITY_MB': memory,
                       'DISK_CAPACITY_GB': disk}
                
            # create event
            machine_events[(create_time, hostname, 'CREATE')] = content
            # update event
            machine_events[(node_update_time, hostname, 'UPDATE')] = content
            # delete event
            if delete_time:
                machine_events[(delete_time, hostname, 'DELETE')] = content
                # disable event
            if disabled == 1:
                machine_events[(service_update_time, hostname, 'DISABLE')] = content
            # enable event
            if disabled == 0:
                machine_events[(service_update_time, hostname, 'ENABLE')] = content
    except:
        LOG.exception("Failed to extract data from {}".format(nova_tmp_database_name))
            
    # clean up
    db.cursor.execute('DROP DATABASE {}'.format(nova_tmp_database_name))
    shutil.rmtree(tmp_path) 
    
    return machine_events
    