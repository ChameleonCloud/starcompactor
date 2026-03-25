# coding: utf-8
"""Extractor for instance action events from openstack_audit parquet files.

Produces trace dicts compatible with the existing mysql.TRACE_EVENT_KEY_RENAME_MAP
field names so they can flow through the same transform pipeline.

The audit parquet files wrap each row's payload as a JSON string in a 'data'
column, identical to the machine event audit files.  The relevant tables are:
  openstack_audit.audit_nova_instances.parquet
  openstack_audit.audit_nova_instance_actions.parquet
  openstack_audit.audit_nova_instance_actions_events.parquet
"""
import json
import logging
import os

import pandas as pd
from dateutil.parser import parse as dateparse

LOG = logging.getLogger(__name__)


def _parse_audit_parquet(path):
    """Read an audit parquet file, parse JSON payloads, and return a DataFrame.

    Each audit row's 'data' column is a JSON-encoded dict of the original DB
    row.  This mirrors _read_and_load_parquet in machine.py.
    """
    df = pd.read_parquet(path)
    rows = []
    for _, row in df.iterrows():
        payload = json.loads(row['data'])
        payload['audit_event_type'] = row['audit_event_type']
        payload['audit_changed_at'] = row['audit_changed_at']
        rows.append(payload)
    return pd.DataFrame(rows)


def _to_dt(val):
    """Convert a value to a naive Python datetime, or None."""
    if val is None:
        return None
    if isinstance(val, pd.Timestamp):
        if pd.isnull(val):
            return None
        dt = val.to_pydatetime()
    else:
        try:
            dt = dateparse(str(val))
        except Exception:
            return None
    return dt.replace(tzinfo=None) if dt.tzinfo is not None else dt


def get_instance_events_from_parquet(data_dir, start=None, end=None, instance_type=None):
    """Read instance action events from openstack_audit parquet files and yield trace dicts.

    Replicates the SQL JOIN:
        instances AS i
        JOIN instance_actions AS ia ON i.uuid = ia.instance_uuid
        JOIN instance_actions_events AS iae ON ia.id = iae.action_id
    WHERE i.user_id != 'admin' AND i.project_id != 'admin'
    AND (optional) ia.created_at >= start AND ia.created_at <= end
    AND iae.finish_time IS NOT NULL

    Parameters
    ----------
    data_dir : str
        Directory containing the three openstack_audit nova parquet files.
    start : datetime or None
        Optional lower bound on ia.created_at (action start time).
    end : datetime or None
        Optional upper bound on ia.created_at (action start time).
    instance_type : str or None
        Optional type of instance (e.g. 'baremetal') to determine host selection.

    Yields
    ------
    dict
        One dict per event row with renamed keys matching TRACE_EVENT_KEY_RENAME_MAP.
    """
    instances_path = os.path.join(
        data_dir, 'openstack_audit.audit_nova_instances.parquet')
    actions_path = os.path.join(
        data_dir, 'openstack_audit.audit_nova_instance_actions.parquet')
    events_path = os.path.join(
        data_dir, 'openstack_audit.audit_nova_instance_actions_events.parquet')

    LOG.info('Reading audit_nova_instances from %s', instances_path)
    df_instances = _parse_audit_parquet(instances_path)

    LOG.info('Reading audit_nova_instance_actions from %s', actions_path)
    df_actions = _parse_audit_parquet(actions_path)

    LOG.info('Reading audit_nova_instance_actions_events from %s', events_path)
    df_events = _parse_audit_parquet(events_path)

    # Each audit table may have INSERT and DELETE rows.  For instances we only
    # want the INSERT (creation) record to get the instance metadata.
    # For actions and events there are only INSERTs in practice, but filter
    # defensively so we don't double-count if DELETEs ever appear.
    if 'audit_event_type' in df_instances.columns:
        df_instances = df_instances[df_instances['audit_event_type'] == 'INSERT']
    if 'audit_event_type' in df_actions.columns:
        df_actions = df_actions[df_actions['audit_event_type'] == 'INSERT']
    if 'audit_event_type' in df_events.columns:
        df_events = df_events[df_events['audit_event_type'] == 'INSERT']

    # Drop audit metadata columns before merging
    drop_cols = ['audit_event_type', 'audit_changed_at']
    df_instances = df_instances.drop(columns=[c for c in drop_cols if c in df_instances.columns])
    df_actions   = df_actions.drop(columns=[c for c in drop_cols if c in df_actions.columns])
    df_events    = df_events.drop(columns=[c for c in drop_cols if c in df_events.columns])

    # Filter out admin instances (mirrors SQL WHERE clause)
    df_instances = df_instances[
        (df_instances['user_id'] != 'admin') &
        (df_instances['project_id'] != 'admin')
    ]

    # Keep only columns we need to reduce memory usage
    instance_cols = ['uuid', 'memory_mb', 'root_gb', 'vcpus',
                     'user_id', 'project_id', 'hostname', 'host', 'node']
    df_instances = df_instances[[c for c in instance_cols if c in df_instances.columns]]

    # Deduplicate instances by uuid — keep the first INSERT per uuid
    df_instances = df_instances.drop_duplicates(subset=['uuid'], keep='first')

    # Apply optional time filters on action created_at
    if 'created_at' in df_actions.columns:
        df_actions['created_at'] = pd.to_datetime(df_actions['created_at'], errors='coerce', utc=False)
        if start is not None:
            start_naive = start.replace(tzinfo=None) if start.tzinfo else start
            df_actions = df_actions[df_actions['created_at'] >= start_naive]
        if end is not None:
            end_naive = end.replace(tzinfo=None) if end.tzinfo else end
            df_actions = df_actions[df_actions['created_at'] <= end_naive]

    action_cols = ['id', 'instance_uuid']
    df_actions = df_actions[[c for c in action_cols if c in df_actions.columns]]
    # Deduplicate actions by id
    df_actions = df_actions.drop_duplicates(subset=['id'], keep='first')

    event_cols = ['action_id', 'event', 'start_time', 'finish_time', 'result', 'host']
    df_events = df_events[[c for c in event_cols if c in df_events.columns]]

    # JOIN: instances <-> instance_actions on uuid = instance_uuid
    df = pd.merge(
        df_instances, df_actions,
        left_on='uuid', right_on='instance_uuid',
        how='inner',
        suffixes=('', '_action'),
    )

    # JOIN: result <-> instance_actions_events on id = action_id
    df = pd.merge(
        df, df_events,
        left_on='id', right_on='action_id',
        how='inner',
        suffixes=('', '_event'),
    )

    LOG.info('Total rows after join: %d', len(df))

    n_records = 0
    n_skipped = 0
    for _, row in df.iterrows():
        finish_time_raw = row.get('finish_time')
        # Replicate the SQL extractor's skip of rows with no finish_time
        if finish_time_raw is None or (isinstance(finish_time_raw, float) and pd.isnull(finish_time_raw)):
            LOG.debug('Invalid event (no finish_time): %s', row.get('uuid'))
            n_skipped += 1
            continue
        try:
            finish_time = _to_dt(finish_time_raw)
        except Exception:
            n_skipped += 1
            continue
        if finish_time is None:
            n_skipped += 1
            continue

        host_event = row.get('host_event')
        host_instance = row.get('host')
        if pd.isnull(host_event):
            host_event = None
        if pd.isnull(host_instance):
            host_instance = None

        if instance_type == 'baremetal':
            node_instance = row.get('node')
            if pd.isnull(node_instance):
                node_instance = None
            final_host = node_instance or host_event or host_instance
        else:
            final_host = host_event or host_instance

        # print(f"final_host: {final_host}, host_event: {host_event}, host_instance: {host_instance}, node_instance: {node_instance}")
        if not final_host:
            LOG.debug('Missing host for event: %s', row.to_dict())

        trace = {
            'INSTANCE_UUID':        row.get('uuid'),
            'EVENT':                row.get('event'),
            'START_TIME':           _to_dt(row.get('start_time')),
            'FINISH_TIME':          finish_time,
            'RESULT':               row.get('result'),
            'INSTANCE_NAME':        row.get('hostname'),
            'USER_ID':              row.get('user_id'),
            'PROJECT_ID':           row.get('project_id'),
            'HOST_NAME (PHYSICAL)': final_host,
            # Extra instance fields preserved by the original SQL extractor
            'memory_mb':            row.get('memory_mb'),
            'root_gb':              row.get('root_gb'),
            'vcpus':                row.get('vcpus'),
        }
        n_records += 1
        yield trace

    LOG.info('Yielded %d records (%d skipped, no finish_time)', n_records, n_skipped)
