#!/bin/bash

source venv/bin/activate

RCLONE_BUCKET=usage_new_collector
RCLONE_REMOTE=uc_chameleon
DATA_DIR=./data

mkdir -p $DATA_DIR

# opts for cloud, data dir, and sync flag
while getopts "o:d:s" opt; do
    case $opt in
        o) OS_CLOUD=$OPTARG;;
        d) PARQUET_DATA_DIR=$OPTARG;;
        s) SHOULD_SYNC="true";;
        ?)
            echo "Usage: $0 -o <os_cloud> -d <parquet_data_dir> [-s] <action>"
            echo "action: instance or machine"
            exit 1
            ;;
    esac
done

shift $((OPTIND - 1))
ACTION=${1}

# Validate ACTION is present
if [ -z "$ACTION" ]; then
    echo "Error: action is required"
    echo "Usage: $0 -o <os_cloud> -d <parquet_data_dir> [-s] <action>"
    echo "action: instance or machine"
    exit 1
fi

# Validate ACTION is a valid value
if [ "$ACTION" != "instance" ] && [ "$ACTION" != "machine" ]; then
    echo "Error: action must be 'instance' or 'machine', got '$ACTION'"
    echo "Usage: $0 -o <os_cloud> -d <parquet_data_dir> [-s] <action>"
    echo "action: instance or machine"
    exit 1
fi

# Validate required flags
if [ -z "$PARQUET_DATA_DIR" ]; then
    echo "Error: -d <parquet_data_dir> is required"
    echo "Usage: $0 -o <os_cloud> -d <parquet_data_dir> [-s] <action>"
    echo "action: instance or machine"
    exit 1
fi

if [ "$SHOULD_SYNC" = "true" ]; then
	echo "Syncing data from $RCLONE_REMOTE:$RCLONE_BUCKET to $DATA_DIR"
	rclone copy $RCLONE_REMOTE:$RCLONE_BUCKET $DATA_DIR
fi

if [ -n "${PARQUET_DATA_DIR}" ]; then
	CLOUD_DIR=$(basename $PARQUET_DATA_DIR)
else
	CLOUD_DIR=${OS_CLOUD}
fi

DATESTAMP=$(date +%Y_%m_%d)

OUT_DIR=./chameleon_${CLOUD_DIR}_cloud_trace_${DATESTAMP}
mkdir -p $OUT_DIR

if [ "$ACTION" = "instance" -a -n "${PARQUET_DATA_DIR}" ]; then
	echo "Generating instance events for $OS_CLOUD"
	echo "Storing output in $OUT_DIR"
	time python -m starcompactor.instance_event_dump \
		--use-parquet --instance-type baremetal \
		--parquet-data-dir $PARQUET_DATA_DIR \
		${OUT_DIR}/${CLOUD_DIR}_instance_events.csv 2>&1 \
	| tee ${OUT_DIR}/${CLOUD_DIR}_instance_events.log

fi

if [ "$ACTION" = "machine" -a -n "${PARQUET_DATA_DIR}" ]; then
	echo "Generating machine events for $PARQUET_DATA_DIR"
	echo "Storing output in $OUT_DIR"
    time python -m starcompactor.machine_event_dump \
		--use-parquet --instance-type baremetal \
		--parquet-data-dir $PARQUET_DATA_DIR \
		${OUT_DIR}/${CLOUD_DIR}_machine_events.csv 2>&1 \
	| tee ${OUT_DIR}/${CLOUD_DIR}_machine_events.log
fi

