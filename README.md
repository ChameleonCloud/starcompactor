# Star Compactor

Generate traces from a Nova (Blazar pending) installation for a source of a real workload.

This is a Python version of a Java program developed by Pankaj Saha ([Java version](https://bitbucket.org/psaha4/chameleon/src/cddb6aaa6ac4a348786b1408a63d28290b6a317a/openStack/src/main/java/extractor/Trace.java?at=master&fileviewer=file-view-default)). 

The current version only supports direct DB dumps. The API dumps has requirements of OpenStack Nova API version and configuration settings. 

## Quickstart

### via Nova's HTTP API

* `pip install -r api-requirements.txt`
  * Requests and Dateutil. If you've installed any OS clients, they're probably
    already installed and you can skip this.
* `python -m starcompactor.api_dump --help`
* `source your/deployment/openrc` (only supports Identity API v2.0)
  * The tool will read the `OS_*` env vars.
  * For a full dump, the credentials loaded need to be high/admin-level. 
    It requires Nova API version greater than 2.1 for viewing deleted instances.
    More specifically, action information of deleted instances can be returned for requests starting with microversion 2.21,
    and currently the minimum version of [microversions] is 2.1.
    "all_tenants" may be configurable, and [viewing instance
    actions][api-actions] should be via `policy.json`.
* `python -m starcompactor.api_dump dump.csv`
* (JSON) `python -m starcompactor.api_dump --jsons [ --osrc admin-chi.tacc.rc ] out.jsons`

### via MySQL database access

(unstable, code fragmented)

* `pip install -r db-requirements.txt`
* `python -m starcompactor.db_dump --help`
* `python -m starcompactor.db_dump --host 127.0.0.1 --start 2017-08-01T00:00:00Z --database nova dump.csv`

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

### Instance Action fields

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

Sensitive fields are masked by hashing the original information and a secret that's discarded at the end of the trace generation.

Specifically:

```python
# program initialization
secret = cryptographically_secure_random_bits(256)

# for each value to mask:
masked_value = cryptographic_hash(secret + original_value)
```

## Implementation Details

The information of instances, instance actions, and instance action events is either fetched using Nova HTTP API requests or directly extracted from OpenStack Nova database.
The information from three sections is merged into a compacted version and several derived fields are extracted/calculated. 

Data masking is applied to `INSTANCE_UUID`, `INSTANCE_NAME`, `USER_ID`, `PROJECT_ID` and `HOST_NAME(PHYSICAL)`. 

## Name

CloudTrail was taken.

> compact star (n.) - remnants of stellar evolution, including white dwarfs, neutron stars, and black holes.

From some mix of OpenStack's Nova, Blazar, me watching Crash Course Astronomy, and posters of computational simulations of stellar remnants around Argonne. Traces are more compact than the whole Nova and Blazar database...

[api-actions]: https://developer.openstack.org/api-ref/compute/#list-actions-for-server
[api-instance-details]: https://developer.openstack.org/api-ref/compute/#list-servers-detailed
[microversions]: https://developer.openstack.org/api-guide/compute/microversions.html
