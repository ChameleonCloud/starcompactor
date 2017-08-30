# Star Compactor

Generate traces from a Nova (Blazar pending) installation for a source of a real workload.

Python version of a Java program developed by Pankaj Saha ([Java version](https://bitbucket.org/psaha4/chameleon/src/cddb6aaa6ac4a348786b1408a63d28290b6a317a/openStack/src/main/java/extractor/Trace.java?at=master&fileviewer=file-view-default)). Only supports direct DB dumps.

## Quickstart

### via Nova's HTTP API

* `pip install -r api-requirements.txt`
  * Requests and Dateutil. If you've installed any OS clients, they're probably
    already installed and you can skip this.
* `python -m starcompactor.api_dump --help`
* `source your/deployment/openrc`
  * The tool will read the `OS_*` env vars.
  * For a full dump, the credentials loaded need to be high/admin-level. API
    docs are unclear if [viewing deleted instances][api-instance-details]
    is configurable. "all_tenants" may be configurable, and [viewing instance
    actions][api-actions] should be via `policy.json`.
* `python -m starcompactor.api_dump dump.csv`


### via MySQL database access

* `pip install -r db-requirements.txt`
* `python -m starcompactor.db_dump --help`
* `python -m starcompactor.db_dump --host 127.0.0.1 --start 2017-08-01T00:00:00Z dump.csv`

The tool requires read access to the `instances`, `instance_actions`, and `instance_actions_events` tables in the Nova database. Events are currently not exposed by the HTTP API, so fetching them remotely is not currently possible.


## Trace Schema

### Instance fields
* uuid (str): instance UUID from Nova

* vcpus (str)
* memory_mb (str)
* root_gb (str)

* user_id (str): masked
* project_id (str): masked
* hostname (str): user-defined hostname, masked
* host (str): physical KVM host, masked

### Instance Action fields:
(none)

### Instance Action Event fields
* event (str)
* result (str)

* start_time (str): ISO-8601 format
* finish_time (str)

### Derived fields
* startSec (int): Dataset-relative epoch (or hard-coded to 2015-09-06 23:31:16)
* finishSec (int)
* duration (int): finish - start

### Missing/Ignored fields
* id (str): from instance_action table, should always equal the action_id
* instance_uuid (str): from instance_action table, should always equal uuid
* action_id (str): from instance_action_events

## Data Masking

Sensitive fields are masked by hashing the original information and a fixed (per-run) random salt that's discarded at the end of the trace generation.

Specifically:

```
# program initialization
salt = cryptographically_secure_random_bits(256)

# for field to mask:
masked = hash(salt + original_data)
```

## Implementation Details

Requires

## Name

CloudTrail was taken.

> compact star (n.) - remnants of stellar evolution, including white dwarfs, neutron stars, and black holes.

From some mix of OpenStack's Nova, Blazar, me watching Crash Course Astronomy, and computational simulations of various stellar remnants. Traces are more compact than the whole infrastructure, or something.

[api-actions]: https://developer.openstack.org/api-ref/compute/#list-actions-for-server
[api-instance-details]: https://developer.openstack.org/api-ref/compute/#list-servers-detailed
