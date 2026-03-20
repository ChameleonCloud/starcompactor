# coding: utf-8
import bz2
import configparser
import datetime
import gzip
import logging
import os
import re
import shutil
import subprocess
import tempfile
import json
import pandas as pd
from dateutil.parser import parse as dateparse

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
            with open(os.path.join(tmp_path, tmp_sql_file_name), 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
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
    process_ironic = subprocess.Popen('mysql --user={} --password={} --host {} --port {} --init-command="SET SESSION FOREIGN_KEY_CHECKS=0;" --one-database {} < {}'.format(mysql_args['user'], mysql_args['passwd'], mysql_args['host'], mysql_args['port'], ironic_tmp_database_name, os.path.join(tmp_path, tmp_ironic_node_table_sql_file_name)), shell=True)
    process_ironic.wait()
    
    tmp_blazar_computehosts_table_sql_file_name = 'blazar_computehosts_{}'.format(tmp_sql_file_name)
    process_extract_computehosts_table = subprocess.Popen("sed -n -e '/DROP TABLE.*`computehosts`/,/UNLOCK TABLES/p' {} > {}".format(os.path.join(tmp_path, tmp_blazar_database_sql_file_name), os.path.join(tmp_path, tmp_blazar_computehosts_table_sql_file_name)), shell=True)
    process_extract_computehosts_table.wait()
    process_blazar = subprocess.Popen('mysql --user={} --password={} --host {} --port {} --init-command="SET SESSION FOREIGN_KEY_CHECKS=0;" --one-database {} < {}'.format(mysql_args['user'], mysql_args['passwd'], mysql_args['host'], mysql_args['port'], blazar_tmp_database_name, os.path.join(tmp_path, tmp_blazar_computehosts_table_sql_file_name)), shell=True)
    process_blazar.wait()
    
    tmp_blazar_computehost_extra_capabilities_table_sql_file_name = 'blazar_computehost_extra_capabilities_{}'.format(tmp_sql_file_name)
    process_extract_computehost_extra_capabilities_table = subprocess.Popen("sed -n -e '/DROP TABLE.*`computehost_extra_capabilities`/,/UNLOCK TABLES/p' {} > {}".format(os.path.join(tmp_path, tmp_blazar_database_sql_file_name), os.path.join(tmp_path, tmp_blazar_computehost_extra_capabilities_table_sql_file_name)), shell=True)
    process_extract_computehost_extra_capabilities_table.wait()
    process_blazar = subprocess.Popen('mysql --user={} --password={} --host {} --port {} --init-command="SET SESSION FOREIGN_KEY_CHECKS=0;" --one-database {} < {}'.format(mysql_args['user'], mysql_args['passwd'], mysql_args['host'], mysql_args['port'], blazar_tmp_database_name, os.path.join(tmp_path, tmp_blazar_computehost_extra_capabilities_table_sql_file_name)), shell=True)
    process_blazar.wait()
    
    tmp_blazar_extra_capabilities_table_sql_file_name = 'blazar_extra_capabilities_{}'.format(tmp_sql_file_name)
    process_extract_extra_capabilities_table = subprocess.Popen("sed -n -e '/DROP TABLE.*`extra_capabilities`/,/UNLOCK TABLES/p' {} > {}".format(os.path.join(tmp_path, tmp_blazar_database_sql_file_name), os.path.join(tmp_path, tmp_blazar_extra_capabilities_table_sql_file_name)), shell=True)
    process_extract_extra_capabilities_table.wait()
    process_blazar = subprocess.Popen('mysql --user={} --password={} --host {} --port {} --init-command="SET SESSION FOREIGN_KEY_CHECKS=0;" --one-database {} < {}'.format(mysql_args['user'], mysql_args['passwd'], mysql_args['host'], mysql_args['port'], blazar_tmp_database_name, os.path.join(tmp_path, tmp_blazar_extra_capabilities_table_sql_file_name)), shell=True)
    process_blazar.wait()
    
    extract_data_sql_old = '''
                           SELECT c.created_at AS create_date, e.created_at, e.updated_at, i.updated_at, i.uuid AS node_id, i.maintenance, e.capability_name, e.capability_value
                           FROM {ironic_database_name}.nodes AS i
                           JOIN {blazar_database_name}.computehosts AS c ON i.uuid = c.hypervisor_hostname
                           JOIN {blazar_database_name}.computehost_extra_capabilities AS e ON e.computehost_id = c.id
                           WHERE capability_name IN ({properties})
                           ORDER BY node_id
                           '''.format(ironic_database_name=ironic_tmp_database_name, 
                                      blazar_database_name=blazar_tmp_database_name, 
                                      properties=','.join("'{}'".format(p) for p in BAREMETAL_PROPERTIES))
                           
    extract_data_sql_new = '''
                           SELECT c.created_at AS create_date, ce.created_at, ce.updated_at, i.updated_at, i.uuid AS node_id, i.maintenance, e.capability_name, ce.capability_value
                           FROM {ironic_database_name}.nodes AS i
                           JOIN {blazar_database_name}.computehosts AS c ON i.uuid = c.hypervisor_hostname
                           JOIN {blazar_database_name}.computehost_extra_capabilities AS ce ON ce.computehost_id = c.id
                           JOIN {blazar_database_name}.extra_capabilities AS e ON e.id = ce.capability_id
                           WHERE capability_name IN ({properties})
                           ORDER BY node_id
                           '''.format(ironic_database_name=ironic_tmp_database_name, 
                                      blazar_database_name=blazar_tmp_database_name, 
                                      properties=','.join("'{}'".format(p) for p in BAREMETAL_PROPERTIES))
    
                       
    try:
        try:
            db.cursor.execute(extract_data_sql_old)
        except:
            db.cursor.execute(extract_data_sql_new)
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
            process_nova = subprocess.Popen('mysql --user={} --password={} --host {} --port {} --init-command="SET SESSION FOREIGN_KEY_CHECKS=0;" --one-database {} < {}'.format(mysql_args['user'], mysql_args['passwd'], mysql_args['host'], mysql_args['port'], nova_tmp_database_name, os.path.join(tmp_path, tmp_table_sql_file_name)), shell=True)
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

def get_machine_events_from_parquet_vm(data_dir):
    machine_events = {} # key is tuple (event_time, host_name, event)
    hosts = set()
    
    # 1. Load services audit
    services_path = os.path.join(data_dir, 'openstack_audit.audit_nova_services.parquet')
    df_services = pd.read_parquet(services_path)
    s_rows = []
    for _, row in df_services.iterrows():
        payload = json.loads(row['data'])
        if payload.get('binary') == NOVA_COMPUTE_HOST_BINARY:
            audit_ts = row['audit_changed_at']
            audit_time = audit_ts.to_pydatetime() if isinstance(audit_ts, pd.Timestamp) else dateparse(str(audit_ts))
            audit_time = audit_time.replace(tzinfo=None)
            s_rows.append({
                'host': payload.get('host'),
                'service_updated_at': audit_time, 
                'disabled': payload.get('disabled') == 1,
            })
    df_s = pd.DataFrame(s_rows)
    if not df_s.empty:
        df_s.sort_values('service_updated_at', inplace=True)
            
    # 2. Load compute nodes audit
    cn_path = os.path.join(data_dir, 'openstack_audit.audit_nova_compute_nodes.parquet')
    df_cn = pd.read_parquet(cn_path)
    cn_rows = []
    for _, row in df_cn.iterrows():
        payload = json.loads(row['data'])
        audit_ts = row['audit_changed_at']
        audit_time = audit_ts.to_pydatetime() if isinstance(audit_ts, pd.Timestamp) else dateparse(str(audit_ts))
        audit_time = audit_time.replace(tzinfo=None)
        
        host = payload.get('hypervisor_hostname') or payload.get('host')
        
        created_at_payload = payload.get('created_at')
        created_at = dateparse(created_at_payload).replace(tzinfo=None) if created_at_payload else audit_time
        
        deleted_at_payload = payload.get('deleted_at')
        if deleted_at_payload:
            deleted_at = dateparse(deleted_at_payload).replace(tzinfo=None)
        else:
            deleted_at = audit_time if row['audit_event_type'] == 'DELETE' else None
                
        cn_rows.append({
            'node_updated_at': audit_time,
            'created_at': created_at,
            'deleted_at': deleted_at,
            'host': host,
            'vcpus': payload.get('vcpus'),
            'memory_mb': payload.get('memory_mb'),
            'local_gb': payload.get('local_gb'),
        })
        
    df_cn_parsed = pd.DataFrame(cn_rows)
    if not df_cn_parsed.empty:
        df_cn_parsed.sort_values('node_updated_at', inplace=True)
        
    # Pandas equivalent of a LEFT JOIN on host over time
    if not df_cn_parsed.empty and not df_s.empty:
        df_joined = pd.merge_asof(
            df_cn_parsed, df_s,
            left_on='node_updated_at', right_on='service_updated_at',
            by='host', direction='backward'
        )
    else:
        df_joined = df_cn_parsed
        df_joined['disabled'] = False
        df_joined['service_updated_at'] = None
        
    for _, row in df_joined.iterrows():
        create_time = row.get('created_at')
        node_update_time = row.get('node_updated_at')
        delete_time = row.get('deleted_at')
        service_update_time = row.get('service_updated_at')
        disabled = row.get('disabled')
        hostname = row.get('host')
        vcpus = row.get('vcpus')
        memory = row.get('memory_mb')
        disk = row.get('local_gb')
        
        if not hostname:
            continue
            
        rack = 'UNKNOWN'
        if RACK_EXTRACTOR['hypervisor_hostname_regex'] and RACK_EXTRACTOR['rack_extract_group']:
            m = re.search(RACK_EXTRACTOR['hypervisor_hostname_regex'], hostname)
            if m:
                rack = m.group(RACK_EXTRACTOR['rack_extract_group'])
                
        content = {
            'rack': rack,
            'vcpu_capability': vcpus,
            'memory_capability_mb': memory,
            'disk_capability_gb': disk
        }
            
        # Identical to original extraction loop
        machine_events[(create_time, hostname, 'CREATE')] = content
        machine_events[(node_update_time, hostname, 'UPDATE')] = content
        if pd.notnull(delete_time):
            machine_events[(delete_time, hostname, 'DELETE')] = {}
        if disabled:
            if pd.notnull(service_update_time):
                machine_events[(service_update_time, hostname, 'DISABLE')] = content
        else:
            if pd.notnull(service_update_time):
                machine_events[(service_update_time, hostname, 'ENABLE')] = content
                
        hosts.add(hostname)
        
    return machine_events, hosts


def _parse_audit_timestamp(ts):
    """Parse an audit timestamp to a naive datetime, handling both Timestamps and strings."""
    if isinstance(ts, pd.Timestamp):
        return ts.to_pydatetime().replace(tzinfo=None)
    return dateparse(str(ts)).replace(tzinfo=None)


def _read_and_load_parquet(path):
    """Read a parquet audit file and return a DataFrame with payload fields plus
    audit_changed_at and audit_event_type preserved from the parquet row."""
    df = pd.read_parquet(path)
    rows = []
    for _, row in df.iterrows():
        payload = json.loads(row['data'])
        payload['audit_changed_at'] = _parse_audit_timestamp(row['audit_changed_at'])
        payload['audit_event_type'] = row['audit_event_type']
        rows.append(payload)
    return pd.DataFrame(rows)


def get_machine_events_from_parquet_baremetal(data_dir):
    machine_events = {}  # key is tuple (event_time, node_id, event)
    hosts = set()

    nodes_path = os.path.join(data_dir, 'openstack_audit.audit_ironic_nodes.parquet')
    blazar_hosts_path = os.path.join(data_dir, 'openstack_audit.audit_blazar_computehosts.parquet')
    blazar_host_capabilities_path = os.path.join(data_dir, 'openstack_audit.audit_blazar_computehost_extra_capabilities.parquet')
    blazar_capabilities_path = os.path.join(data_dir, 'openstack_audit.audit_blazar_resource_properties.parquet')

    nodes = _read_and_load_parquet(nodes_path)
    blazar_hosts = _read_and_load_parquet(blazar_hosts_path)
    blazar_host_capabilities = _read_and_load_parquet(blazar_host_capabilities_path)
    blazar_capabilities = _read_and_load_parquet(blazar_capabilities_path)

    # Filter capabilities to only the properties we care about (mirrors the SQL WHERE clause)
    blazar_capabilities = blazar_capabilities[blazar_capabilities['property_name'].isin(BAREMETAL_PROPERTIES)]

    # Join nodes -> blazar_hosts on uuid = hypervisor_hostname
    # Suffixes avoid collisions on shared column names (created_at, updated_at, etc.)
    df = pd.merge(nodes, blazar_hosts,
                  left_on='uuid', right_on='hypervisor_hostname',
                  suffixes=('_node', '_blazar_host'))

    # Join -> blazar_host_capabilities on computehost_id = blazar_hosts.id
    # blazar_hosts.id came through as id_blazar_host after the previous merge
    df = pd.merge(df, blazar_host_capabilities,
                  left_on='id_blazar_host', right_on='computehost_id',
                  suffixes=('', '_blazar_cap'))

    # Join -> blazar_capabilities (resource_properties) on property_id = id
    df = pd.merge(df, blazar_capabilities,
                  left_on='property_id', right_on='id',
                  suffixes=('_cap', '_resource_prop'))

    # At this point the columns we need are:
    #   create_date      -> created_at_blazar_host   (when the host was registered in Blazar)
    #   node_id          -> uuid                      (ironic node UUID)
    #   maintenance      -> maintenance_node           (ironic maintenance flag)
    #   node_updated_at  -> audit_changed_at_node      (audit timestamp of the ironic node row)
    #   cap_updated_at   -> audit_changed_at_cap       (audit timestamp of the capability row,
    #                                                   used as the UPDATE event time)
    #   capability_name  -> property_name              (from resource_properties)
    #   capability_value -> capability_value            (from computehost_extra_capabilities)

    # Group by node so we can collect all properties and derive per-node event times,
    # mirroring the SQL version's node-by-node loop.
    for node_id, group in df.groupby('uuid'):
        # CREATE time: created_at from blazar computehosts (equivalent to SQL create_date)
        create_date_raw = group['created_at_blazar_host'].iloc[0]
        if pd.isnull(create_date_raw):
            create_date = None
        elif isinstance(create_date_raw, str):
            create_date = dateparse(create_date_raw).replace(tzinfo=None)
        else:
            create_date = create_date_raw

        # UPDATE time: max audit_changed_at across all capability rows for this node
        # (audit_changed_at is already a naive datetime, set by _read_and_load_parquet)
        # After merges the capability audit timestamp is in audit_changed_at_cap
        cap_audit_times = group['audit_changed_at_cap'].dropna()
        update_time = cap_audit_times.max() if not cap_audit_times.empty else create_date

        # Collect property dict for CREATE / UPDATE events
        property_collections = {
            row['property_name']: row['capability_value']
            for _, row in group.iterrows()
            if row['property_name'] in BAREMETAL_PROPERTIES
        }

        # Emit CREATE event
        if create_date is not None:
            machine_events[(create_date, node_id, 'CREATE')] = property_collections

        # Emit UPDATE event
        if update_time is not None:
            machine_events[(update_time, node_id, 'UPDATE')] = property_collections

        # Emit DISABLE / ENABLE events, one per distinct (node_updated_at, maintenance) pair.
        # Each ironic audit row can have its own maintenance state at its own timestamp,
        # so we deduplicate by (audit_changed_at_node, maintenance) to avoid overwriting
        # events that were already written with the same key.
        for _, row in group.drop_duplicates(subset=['audit_changed_at_node', 'maintenance']).iterrows():
            node_update_time = row['audit_changed_at_node']
            is_maint = row['maintenance'] == 1

            event_type = 'DISABLE' if is_maint else 'ENABLE'
            maint_key = (node_update_time, node_id, event_type)

            # Mirror SQL version: only write the first occurrence of a given key
            if maint_key not in machine_events:
                machine_events[maint_key] = {}
            # Accumulate properties onto the event (SQL version builds the dict incrementally)
            machine_events[maint_key][row['property_name']] = row['capability_value']

    return machine_events, hosts