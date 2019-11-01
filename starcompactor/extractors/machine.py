# coding: utf-8
from __future__ import absolute_import, division, print_function
import bz2
import datetime
import gzip
import logging
import os
import re
import shutil
import subprocess
import tempfile

try:
    import configparser # 3.x
except ImportError:
    from backports import configparser # 2.x 3rd party

from . import mysql

LOG = logging.getLogger(__name__)

NOVA_COMPUTE_HOST_BINARY = 'nova-compute'

config = configparser.ConfigParser()
config.read('starcompactor.config')

RACK_EXTRACTOR = {'hypervisor_hostname_regex': config.get('kvm', 'hypervisor_hostname_regex'), 
                  'rack_extract_group': int(config.get('kvm', 'rack_extract_group')) if config.get('kvm', 'rack_extract_group') else None}
NOVA_DATABASES = config.get('default', 'nova_databases').split(',')
BAREMETAL_PROPERTIES = config.get('baremetal', 'properties').split(',')
BACKUP_FILE_REDUCABLE_PREFIX_LEN = int(config.get('backup', 'backup_file_reducable_prefix_len')) if config.get('backup', 'backup_file_reducable_prefix_len') else 0
BACKUP_FILE_REDUCABLE_SUFFIX_LEN = int(config.get('backup', 'backup_file_reducable_suffix_len')) if config.get('backup', 'backup_file_reducable_suffix_len') else 0

def open_by_suffix(filename):
    if filename.endswith('.gz'):
        return gzip.open(filename, 'r')
    elif filename.endswith('.bz2'):
        return bz2.BZ2File(filename, 'r')
    else:
        return open(filename, 'r')

def get_machine_event(process_no, backup_file, mysql_args, instance_type):
    LOG.debug("Process {}: parsing file {}".format(str(process_no), backup_file))
    
    tmp_path = tempfile.mkdtemp()
    filename_len = len(os.path.basename(backup_file))
    tmp_sql_file_name = 'tmp_{}.sql'.format(os.path.basename(backup_file)[0:(filename_len - BACKUP_FILE_REDUCABLE_SUFFIX_LEN)][BACKUP_FILE_REDUCABLE_PREFIX_LEN:]
                                            .replace('.', '_')
                                            .replace('-', '_'))
    
    try:
        with open_by_suffix(backup_file) as f_in:
            filedata = f_in.read()
            with open(os.path.join(tmp_path, tmp_sql_file_name), 'w') as f_out:
                f_out.write(filedata)
    except:
        LOG.exception("Failed to read {}".format(backup_file))
        shutil.rmtree(tmp_path) 
        return {}
    
    if instance_type == 'vm':
        machine_events, hosts = get_machine_event_vm(mysql_args, tmp_path, tmp_sql_file_name)
    elif instance_type == 'baremetal':
        machine_events, hosts = get_machine_event_baremetal(mysql_args, tmp_path, tmp_sql_file_name)
    else:
        raise ValueError('unknown instance type {}'.format(instance_type))
    
    shutil.rmtree(tmp_path) 
    
    return {'machine_events': machine_events, 'hosts': hosts, 'file_time': datetime.datetime.fromtimestamp(os.path.getmtime(backup_file))}

def get_machine_event_baremetal(mysql_args, tmp_path, tmp_sql_file_name):
    machine_events = {} # key is tuple (event_time, host_name, event)
    hosts = set()
    db = mysql.MySqlShim(**mysql_args)
    
    ironic_tmp_database_name = 'baremetal_ironic_backup_{}'.format(os.path.basename(tmp_sql_file_name).split('.')[0])
    blazar_tmp_database_name = 'baremetal_blazar_backup_{}'.format(os.path.basename(tmp_sql_file_name).split('.')[0])
                
    db.cursor.execute('DROP DATABASE IF EXISTS {}'.format(ironic_tmp_database_name))
    db.cursor.execute('CREATE DATABASE {}'.format(ironic_tmp_database_name)) 
    db.cursor.execute('DROP DATABASE IF EXISTS {}'.format(blazar_tmp_database_name))
    db.cursor.execute('CREATE DATABASE {}'.format(blazar_tmp_database_name)) 
        
    tmp_ironic_database_sql_file_name = 'ironic_{}'.format(tmp_sql_file_name)
    tmp_blazar_database_sql_file_name = 'blazar_{}'.format(tmp_sql_file_name)
    
    process_extract_ironic_database = subprocess.Popen("sed -n -e '/^USE `ironic`/,/^USE/p' {} > {}".format(os.path.join(tmp_path, tmp_sql_file_name), os.path.join(tmp_path, tmp_ironic_database_sql_file_name)), shell=True)
    process_extract_ironic_database.wait()
    process_extract_ironic_database = subprocess.Popen("sed -n -e '/^USE `blazar`/,/^USE/p' {} > {}".format(os.path.join(tmp_path, tmp_sql_file_name), os.path.join(tmp_path, tmp_blazar_database_sql_file_name)), shell=True)
    process_extract_ironic_database.wait()
        
    tmp_ironic_node_table_sql_file_name = 'ironic_nodes_{}'.format(tmp_sql_file_name)
    process_extract_node_table = subprocess.Popen("sed -n -e '/DROP TABLE.*`nodes`/,/UNLOCK TABLES/p' {} > {}".format(os.path.join(tmp_path, tmp_ironic_database_sql_file_name), os.path.join(tmp_path, tmp_ironic_node_table_sql_file_name)), shell=True)
    process_extract_node_table.wait()
    process_ironic = subprocess.Popen('mysql --user={} --password={} --init-command="SET SESSION FOREIGN_KEY_CHECKS=0;" --one-database {} < {}'.format(mysql_args['user'], mysql_args['passwd'], ironic_tmp_database_name, os.path.join(tmp_path, tmp_ironic_node_table_sql_file_name)), shell=True)
    process_ironic.wait()
    
    tmp_blazar_computehosts_table_sql_file_name = 'blazar_computehosts_{}'.format(tmp_sql_file_name)
    process_extract_computehosts_table = subprocess.Popen("sed -n -e '/DROP TABLE.*`computehosts`/,/UNLOCK TABLES/p' {} > {}".format(os.path.join(tmp_path, tmp_blazar_database_sql_file_name), os.path.join(tmp_path, tmp_blazar_computehosts_table_sql_file_name)), shell=True)
    process_extract_computehosts_table.wait()
    process_blazar = subprocess.Popen('mysql --user={} --password={} --init-command="SET SESSION FOREIGN_KEY_CHECKS=0;" --one-database {} < {}'.format(mysql_args['user'], mysql_args['passwd'], blazar_tmp_database_name, os.path.join(tmp_path, tmp_blazar_computehosts_table_sql_file_name)), shell=True)
    process_blazar.wait()
    
    tmp_blazar_computehost_extra_capabilities_table_sql_file_name = 'blazar_computehost_extra_capabilities_{}'.format(tmp_sql_file_name)
    process_extract_computehost_extra_capabilities_table = subprocess.Popen("sed -n -e '/DROP TABLE.*`computehost_extra_capabilities`/,/UNLOCK TABLES/p' {} > {}".format(os.path.join(tmp_path, tmp_blazar_database_sql_file_name), os.path.join(tmp_path, tmp_blazar_computehost_extra_capabilities_table_sql_file_name)), shell=True)
    process_extract_computehost_extra_capabilities_table.wait()
    process_blazar = subprocess.Popen('mysql --user={} --password={} --init-command="SET SESSION FOREIGN_KEY_CHECKS=0;" --one-database {} < {}'.format(mysql_args['user'], mysql_args['passwd'], blazar_tmp_database_name, os.path.join(tmp_path, tmp_blazar_computehost_extra_capabilities_table_sql_file_name)), shell=True)
    process_blazar.wait()
    
    extract_data_sql = '''
                       SELECT c.created_at AS create_date, e.created_at, e.updated_at, i.updated_at, i.uuid AS node_id, i.maintenance, e.capability_name, e.capability_value
                       FROM {ironic_database_name}.nodes AS i
                       JOIN {blazar_database_name}.computehosts AS c ON i.uuid = c.hypervisor_hostname
                       JOIN {blazar_database_name}.computehost_extra_capabilities AS e ON e.computehost_id = c.id
                       WHERE capability_name IN ({properties})
                       ORDER BY node_id
                       '''.format(ironic_database_name=ironic_tmp_database_name, 
                                  blazar_database_name=blazar_tmp_database_name, 
                                  properties=','.join("'{}'".format(p) for p in BAREMETAL_PROPERTIES))
                       
    try:
        db.cursor.execute(extract_data_sql)
        prev_node = None
        prev_create_time = None
        property_update_times = []
        property_collections = {}
        for row in db.cursor:
            create_time = row[0]
            property_create_time = row[1]
            property_update_time = row[2]
            node_update_time = row[3]
            node_id = row[4]
            is_maint = row[5] == 1
            property_name = row[6]
            property_value = row[7]
            
            if prev_node and prev_node != node_id:
                # create event
                machine_events[(prev_create_time, prev_node, 'CREATE')] = property_collections
                # update event
                machine_events[(max(property_update_times), prev_node, 'UPDATE')] = property_collections
                
                property_update_times = []
                property_collections = {}
            
            prev_node = node_id
            prev_create_time = create_time
            property_collections[property_name] = property_value
            property_update_times.append(property_create_time)
            if property_update_time: property_update_times.append(property_update_time)
            
            # disable event (maintenance)
            if is_maint:
                maint_key = (node_update_time, node_id, 'DISABLE')
            # enable event (not maintenance)
            else:
                maint_key = (node_update_time, node_id, 'ENABLE')
            if maint_key not in machine_events:
                machine_events[maint_key] = {}
            machine_events[maint_key][property_name] = property_value
        # create event
        machine_events[(prev_create_time, prev_node, 'CREATE')] = property_collections
        # update event
        machine_events[(max(property_update_times), prev_node, 'UPDATE')] = property_collections
    except:
        LOG.exception("Failed to extract data from OpenStack databases")
                
    # clean up
    db.cursor.execute('DROP DATABASE {}'.format(ironic_tmp_database_name))
    db.cursor.execute('DROP DATABASE {}'.format(blazar_tmp_database_name))
    
    return machine_events, hosts
    
def get_machine_event_vm(mysql_args, tmp_path, tmp_sql_file_name):
    machine_events = {} # key is tuple (event_time, host_name, event)
    hosts = set()
    db = mysql.MySqlShim(**mysql_args)
    
    for database in NOVA_DATABASES:
        nova_tmp_database_name = 'kvm_{}_backup_{}'.format(database, os.path.basename(tmp_sql_file_name).split('.')[0])
                
        db.cursor.execute('DROP DATABASE IF EXISTS {}'.format(nova_tmp_database_name))
        db.cursor.execute('CREATE DATABASE {}'.format(nova_tmp_database_name)) 
        
        tmp_database_sql_file_name = '{}_{}'.format(database, tmp_sql_file_name)
        process_extract_database = subprocess.Popen("sed -n -e '/^USE `{}`/,/^USE/p' {} > {}".format(database, os.path.join(tmp_path, tmp_sql_file_name), os.path.join(tmp_path, tmp_database_sql_file_name)), shell=True)
        process_extract_database.wait()
        
        for table in ['compute_nodes', 'services']: 
            tmp_table_sql_file_name = '{}_{}'.format(table, tmp_sql_file_name)
            process_extract_table = subprocess.Popen("sed -n -e '/DROP TABLE.*`{}`/,/UNLOCK TABLES/p' {} > {}".format(table, os.path.join(tmp_path, tmp_database_sql_file_name), os.path.join(tmp_path, tmp_table_sql_file_name)), shell=True)
            process_extract_table.wait()
            process_nova = subprocess.Popen('mysql --user={} --password={} --init-command="SET SESSION FOREIGN_KEY_CHECKS=0;" --one-database {} < {}'.format(mysql_args['user'], mysql_args['passwd'], nova_tmp_database_name, os.path.join(tmp_path, tmp_table_sql_file_name)), shell=True)
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
                disabled = row[4] == 1
                binary = row[5]
                hostname = row[6]
                vcpus = row[7]
                memory = row[8]
                disk = row[9]
                
                if not binary or binary != NOVA_COMPUTE_HOST_BINARY:
                    continue
                rack = 'UNKNOWN'
                if RACK_EXTRACTOR['hypervisor_hostname_regex'] and RACK_EXTRACTOR['rack_extract_group']:
                    m = re.search(RACK_EXTRACTOR['hypervisor_hostname_regex'], row[6])
                    if m:
                        rack = m.group(RACK_EXTRACTOR['rack_extract_group'])
                content = {'rack': rack,
                           'vcpu_capability': vcpus,
                           'memory_capability_mb': memory,
                           'disk_capability_gb': disk}
                    
                # create event
                machine_events[(create_time, hostname, 'CREATE')] = content
                # update event
                machine_events[(node_update_time, hostname, 'UPDATE')] = content
                # delete event
                if delete_time:
                    machine_events[(delete_time, hostname, 'DELETE')] = {}
                # disable event
                if disabled:
                    machine_events[(service_update_time, hostname, 'DISABLE')] = content
                # enable event
                else:
                    machine_events[(service_update_time, hostname, 'ENABLE')] = content
                hosts.add(hostname)
        except:
            LOG.exception("Failed to extract data from {}".format(nova_tmp_database_name))
                
        # clean up
        db.cursor.execute('DROP DATABASE {}'.format(nova_tmp_database_name))
    
    return machine_events, hosts
    