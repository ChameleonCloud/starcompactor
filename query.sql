SELECT
    i.uuid,
    i.memory_mb,
    i.root_gb,
    i.vcpus,
    i.user_id,
    i.project_id,
    i.hostname,
    i.host,
    -- ia.created_at,
    -- ia.id AS action_id,
    -- iae.id AS event_id,
    iae.event,
    iae.result,
    iae.start_time,
    iae.finish_time
FROM
    nova.instances AS i
        JOIN
    nova.instance_actions AS ia ON i.uuid = ia.instance_uuid
        JOIN
    nova.instance_actions_events AS iae ON ia.id = iae.action_id
WHERE
    ia.created_at > %s
    AND ia.created_at < %s
LIMIT 1000;
