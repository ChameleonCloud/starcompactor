# Star Compactor

Generate traces from the Nova, Blazar and Ironic installations for a source of a real workload.

This Python program is developed based on a Java program developed by Pankaj Saha ([Java version](https://bitbucket.org/psaha4/chameleon/src/cddb6aaa6ac4a348786b1408a63d28290b6a317a/openStack/src/main/java/extractor/Trace.java?at=master&fileviewer=file-view-default)). 

## Prerequisites
* Create a mysql database user `cc_trace` with the following privileges:
	
	``GRANT SELECT, INSERT, CREATE, DROP, ALTER, LOCK TABLES ON `baremetal_ironic_backup\_%`.* TO 'cc_trace'@'%'``
	
	``GRANT SELECT, INSERT, CREATE, DROP, ALTER, LOCK TABLES ON `baremetal_blazar_backup\_%`.* TO 'cc_trace'@'%'``
	
	``GRANT SELECT, INSERT, CREATE, DROP, ALTER, LOCK TABLES ON `kvm\_%_backup\_%`.* TO 'cc_trace'@'%'``
	
	``GRANT SELECT ON `nova%`.* TO 'cc_trace'@'%'``
	
* Install required python packages using command `pip install -r db-requirements.txt`

* Modify `starcompactor.config` file

```
	[default]
	epoch # epoch time used to calculate the time difference between the event time and the epoch time
	nova_databases # if you use OpenStack Nova Cells, list all cell databases here
	
	[baremetal]
	# baremetal traces related parameters
	properties # comma separated node properties you'd like to included in the machine event trace. 
			   # i.e. the capabilities names from blazar.computehost_extra_capabilities.
	rack_property_name # if you have rack information as one of the node properties, please indicate the name of the rack property. 
					   # it is used for anonymizing rack infomation. otherwise, leave blank.
	
	[backup]
	# machine events are read from backups. use this section to indicate the backup information
	backup_dir # the directory of the backup files
	backup_file_regex # regex to match backup files. if all files under the backup dir, put *.
	backup_file_reducable_prefix_len # the script uses backup file name as the tmp database name. 
								     # if the file name is too long, it may cause error when creating tmp databases. 
								     # use this parameter to tell how many leading characters can be truncated from the file name.
								     # note: the tmp database name has to be unique.
	backup_file_reducable_suffix_len # same purpose as backup_file_reducable_prefix_len, but used to truncate the tailing characters.
	
	[kvm]
	# kvm traces related parameters
	hypervisor_hostname_regex # rack information is extracted from hypervisor_hostname. 
							  # combine with rack_extract_group parameter to extract rack information.
	rack_extract_group
	
	[multithread]
	number_of_files_per_process # machine events is extracted using multithreading.
								# tune this parameter to adjust the number of threads and the number of files per thread
```

* Supported python version -- Python 2 and 3

## Instance Events

The instance events trace includes information about the instances, 
such as the action (event) performed on an OpenStack instance, the event start time, the event end time, the result of the event, physical machine that hosting the instance, etc. 
The instance events trace is extracted directly from the OpenStack Nova databases. To generate instance event trace, run the following command: 

```
python -m starcompactor.instance_event_db_dump --db-user cc_trace --password <cc_trace password> instance_events.csv
```

The script also allows to specify the mysql database host and port, output format, instance type, etc. Run the following to get more help:

```
python -m starcompactor.instance_event_db_dump --help
```

The default instances type is `vm`. To get baremetal instance event trace, add `--instance-type baremetal`.

#### Alternative Way to Create Instance Event Traces
An alternative way of creating instance events is provided -- using [OpenStack API](https://docs.openstack.org/api-quick-start/).
Before generating instance event traces, you need to install required python packages using `pip install -r api-requirements.txt`. 
In addition, make sure you have the [`admin-openrc` file](https://docs.openstack.org/newton/install-guide-ubuntu/keystone-openrc.html) ready to use. 
You can `source` the `openrc` file, or use `--osrc` parameter to setup the required environment variables.

To generate instance event traces using OpenStack API, run the following command:

```
python -m starcompactor.instance_event_api_dump instance_events.csv
```

Run the following to get more help:

```
python -m starcompactor.instance_event_api_dump --help
```

## Machine Events

The machine events trace includes information about the physical hosts. Five types of events are recorded for each machine, including `CREATE`, `DELETE`, `UPDATE`, `ENABLE`, `DISABLE`. 
The machine properties are also included. 

The machine events trace is extracted from OpenStack (Nova for vm; Ironic and Blazar for baremetal) database backups. The script will sort the backup files by its latest modified date, 
and assumes that the earlier the creation date the older the database backup file. Three backup file types are accepted - bz2, gz or plain text.

For the `vm` instance type, we assume that the rack information can be extracted from the hypervisor host name.
You can specify the `hypervisor_hostname_regex` and `rack_extract_group` parameters in the `starcompactor.config` file to tell the script how to parse the rack from the hypervisor host name.
Otherwise, leave the parameters blank and the trace will have all zeros for the `RACK` property.

To generate machine event trace, run the following command:

```
python -m starcompactor.machine_event_dump --db-user cc_trace --password <cc_trace_password> machine_events.csv
```

The script also allows to specify the mysql database host and port, output format, instance type, etc. Run the following to get more help:

```
python -m starcompactor.machine_event_dump --help
```

The default instances type is `vm`. To get baremetal instance event trace, add `--instance-type baremetal`.

## Anonymization Techniques

For confidentiality reasons, you can anonymize certain fields in the traces. We use two different analymization methods on different fields.

### Hashed
You can use keyed cryptographic hash to the fields `INSTANCE_UUID`, `INSTANCE_NAME`, `USER_ID`, `PROJECT_ID` and `HOST_NAME (PHYSICAL)`.
The default hash method is `sha2-salted`, but you can choose `sha1-raw` or `none` (no masking) using `--hashed-masking-method` parameter. 

If you don't specify the hash salt, the script will generate one randomly. 
To allow mutual reference between instance events and machine events, you need to apply the same hash key (salt) to the host name fields.
Use `-hashed-masking-salt` parameter to apply a hash key for masking.

### Ordered
This technique is applied to the RACK property in the machine events table. 
The list of observed unique RACK values is sorted. 
Then, we assigned sequential numbers starting with 0 to the items of the RACK list, and the observed values are mapped onto these numbers.

## Science Clouds
* For more details about trace format and masking techniques, please visit the [trace format page](https://scienceclouds.org/cloud-traces/cloud-trace-format/) at [scienceclouds.org](https://scienceclouds.org). 
* For published Chameleon traces, please visit the [cloud traces page](https://scienceclouds.org/cloud-traces/) at [scienceclouds.org](https://scienceclouds.org).
* There are more published traces at [scienceclouds.org](https://scienceclouds.org/cloud-traces/)!

## Name

CloudTrail was taken.

> compact star (n.) - remnants of stellar evolution, including white dwarfs, neutron stars, and black holes.

From some mix of OpenStack's Nova, Blazar, me watching Crash Course Astronomy, and posters of computational simulations of stellar remnants around Argonne. Traces are more compact than the whole Nova and Blazar database...

[api-actions]: https://developer.openstack.org/api-ref/compute/#list-actions-for-server
[api-instance-details]: https://developer.openstack.org/api-ref/compute/#list-servers-detailed
[microversions]: https://developer.openstack.org/api-guide/compute/microversions.html
