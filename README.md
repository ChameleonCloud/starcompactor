# Star Compactor

Generate traces from a Nova (Blazar pending) installation for a source of a real workload.

This is a Python version of a Java program developed by Pankaj Saha ([Java version](https://bitbucket.org/psaha4/chameleon/src/cddb6aaa6ac4a348786b1408a63d28290b6a317a/openStack/src/main/java/extractor/Trace.java?at=master&fileviewer=file-view-default)). 

## Prerequisites
* Create a mysql database user `cc_trace` with the following privileges:

	``GRANT SELECT, INSERT, CREATE, DROP, ALTER, LOCK TABLES ON `kvm_nova_backup\_%`.* TO 'cc_trace'@'%'``
	
	``GRANT SELECT ON `nova` TO 'cc_trace'@'%'``
	
* Python2
* `pip install -r db-requirements.txt`

## Instance Events

The instance events trace includes information about the instances, 
such as the action (event) performed on an OpenStack instance, the event start time, the event end time, the result of the event, physical machine that hosting the instance, etc. 
The instance events trace is extracted directly from the OpenStack Nova database. To generate instance event trace, run the following command: 

```
python -m starcompactor.db_dump --db-user cc_trace --password <cc_trace password> instance_events.csv
```

The script also allows to specify the mysql database host and port, epoch, output format, etc. Run the following to get more help:

```
python -m starcompactor.db_dump --help
```

## Machine Events

The machine events trace includes information about the physical hosts. Five types of events are recorded for each machine, including `CREATE`, `DELETE`, `UPDATE`, `ENABLE`, `DISABLE`. 
The machine details recorded with the events are `RACK`, `VCPU_CAPACITY`, `MEMORY_CAPACITY_MB`, and `DISK_CAPACITY_GB`. 

The machine events trace is extracted from OpenStack (nova) database backups. The script will sort the backup files by its latest modified date, 
and assumes that the earlier the creation date the older the database backup file. Three backup file types are accepted - bz2, gz or plain text.

To generate the machine events, you need to modify the `starcompactor.config` file first. We assume that the rack information can be extracted from the hypervisor host name.
You can specify the `hypervisor_hostname_regex` and `rack_extract_group` parameters in the `starcompactor.config` file to tell the script how to parse the rack from the hypervisor host name.
Otherwise, leave the parameters blank and the trace will have all zeros for the `RACK` column.

To generate machine event trace, run the following command:

```
python -m starcompactor.machine_event_dump --db-user cc_trace --password <cc_trace_password> machine_events.csv
```

The script also allows to specify the mysql database host and port, epoch, output format, etc. Run the following to get more help:

```
python -m starcompactor.machine_event_dump --help
```

## Anonymization Techniques

For confidentiality reasons, you can anonymize certain fields in the traces. We use two different analymization methods on different fields.

### Hashed
You can use keyed cryptographic hash to the fields `INSTANCE_UUID`, `INSTANCE_NAME`, `USER_ID`, `PROJECT_ID` and `HOST_NAME (PHYSICAL)`.
The default hash method is `sha2-salted`, but you can choose `sha1-raw` or `none` (no masking) by specifying `--masking` parameter with `db_dump` (instance event) script
and specifying `--hashed-masking-method` parameter with `machine_event_dump` (machine events)  script. 

If you don't specify the hash salt, the script will generate one randomly. 
To allow mutual reference between instance events and machine events, you need to apply the same hash key (salt) to the host name fields.
Use `--salt` parameter with `db_dump` (instance_event) script and use `-hashed-masking-salt` parameter with `machine_event_dump` (machine events) script.

### Ordered
This technique is applied to RACK field in the machine events table. 
The list of observed unique RACK values is sorted. 
Then, we assigned sequential numbers starting with 0 to the items of the RACK list, and the observed values are mapped onto these numbers.

## Science Clouds
* For more details about trace format and masking techniques, please visit the [trace format page](https://scienceclouds.org/cloud-traces/cloud-trace-format/) at [scienceclouds.org](https://scienceclouds.org). 
* For published Chameleon KVM traces, please visit the [cloud traces page](https://scienceclouds.org/cloud-traces/) at [scienceclouds.org](https://scienceclouds.org).
* There are more published traces at [scienceclouds.org](https://scienceclouds.org/cloud-traces/)!

## Name

CloudTrail was taken.

> compact star (n.) - remnants of stellar evolution, including white dwarfs, neutron stars, and black holes.

From some mix of OpenStack's Nova, Blazar, me watching Crash Course Astronomy, and posters of computational simulations of stellar remnants around Argonne. Traces are more compact than the whole Nova and Blazar database...

[api-actions]: https://developer.openstack.org/api-ref/compute/#list-actions-for-server
[api-instance-details]: https://developer.openstack.org/api-ref/compute/#list-servers-detailed
[microversions]: https://developer.openstack.org/api-guide/compute/microversions.html
