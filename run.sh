#!/bin/bash

source venv/bin/activate

SALT_VALUE="chameleon"
RCLONE_BUCKET=usage_new_collector
RCLONE_REMOTE=uc_chameleon
DATA_DIR=./data
DATESTAMP=$(date +%Y_%m_%d)
INSTANCE_TYPE="baremetal"

OUT_DIR_BASE=./out

mkdir -p $DATA_DIR

# opts for cloud, data dir, and sync flag
while getopts "o:d:i:sb:" opt; do
    case $opt in
        o) OS_CLOUD=$OPTARG;;
        d) PARQUET_DATA_DIR=$OPTARG;;
        i) INSTANCE_TYPE=$OPTARG;;
        s) SHOULD_SYNC="true";;
        b) OUT_DIR_BASE=$OPTARG;;
        ?)
            echo "Usage: $0 -o <os_cloud> -d <parquet_data_dir> [-s] [-b <out_dir_base>] <action>"
            echo "action: instance, machine, or sync"
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
    echo "action: instance, machine, or sync"
    exit 1
fi

# Validate ACTION is a valid value
if [ "$ACTION" != "instance" ] && [ "$ACTION" != "machine" ] && [ "$ACTION" != "sync" ]; then
    echo "Error: action must be 'instance', 'machine', or 'sync', got '$ACTION'"
    echo "Usage: $0 -o <os_cloud> -d <parquet_data_dir> [-s] <action>"
    echo "action: instance, machine, or sync"
    exit 1
fi

# Validate required flags
if [ -z "$PARQUET_DATA_DIR" -a "$ACTION" != "sync" ]; then
    echo "Error: -d <parquet_data_dir> is required"
    echo "Usage: $0 -o <os_cloud> -d <parquet_data_dir> [-s] <action>"
    echo "action: instance, machine, or sync"
    exit 1
fi

if [ "$SHOULD_SYNC" = "true" -o "$ACTION" = "sync" ]; then
	echo "Syncing data from $RCLONE_REMOTE:$RCLONE_BUCKET to $DATA_DIR"
	rclone copy $RCLONE_REMOTE:$RCLONE_BUCKET $DATA_DIR
fi

if [ -n "${PARQUET_DATA_DIR}" ]; then
	CLOUD_DIR=$(basename $PARQUET_DATA_DIR)
else
	CLOUD_DIR=${OS_CLOUD}
fi

OUT_DIR=${OUT_DIR_BASE}/${CLOUD_DIR}_cloud_trace_${DATESTAMP}

if [ "$ACTION" = "instance" -a -n "${PARQUET_DATA_DIR}" ]; then
	mkdir -p $OUT_DIR
	echo "Generating instance events for $OS_CLOUD"
	echo "Storing output in $OUT_DIR"
	time python -m starcompactor.instance_event_dump \
		--use-parquet --instance-type $INSTANCE_TYPE \
		--parquet-data-dir $PARQUET_DATA_DIR \
        --hashed-masking-salt $SALT_VALUE \
		${OUT_DIR}/${CLOUD_DIR}_instance_events.csv 2>&1 \
	| tee ${OUT_DIR}/${CLOUD_DIR}_instance_events.log
fi

if [ "$ACTION" = "machine" -a -n "${PARQUET_DATA_DIR}" ]; then
	mkdir -p $OUT_DIR
	echo "Generating machine events for $PARQUET_DATA_DIR"
	echo "Storing output in $OUT_DIR"
    time python -m starcompactor.machine_event_dump \
		--use-parquet --instance-type $INSTANCE_TYPE \
		--parquet-data-dir $PARQUET_DATA_DIR \
        --hashed-masking-salt $SALT_VALUE \
		${OUT_DIR}/${CLOUD_DIR}_machine_events.csv 2>&1 \
	| tee ${OUT_DIR}/${CLOUD_DIR}_machine_events.log
fi

