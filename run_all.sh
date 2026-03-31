#!/bin/bash

# Site dir and type map
SITE_MAP=(
    'chi_uc' 'baremetal'
    'chi_tacc' 'baremetal'
    'kvm_tacc' 'vm'
)

i=0
while [ $i -lt ${#SITE_MAP[@]} ]; do
    site=${SITE_MAP[i]}
    instance_type=${SITE_MAP[i+1]}
    echo "Processing site: $site with instance type: $instance_type"
    ./run.sh -s -d ./data/$site/ -i $instance_type -b ./out instance
    ./run.sh -s -d ./data/$site/ -i $instance_type -b ./out machine
    i=$((i+2))
done

pushd ./out
for dir in *; do
    zip -r "$dir.zip" "$dir"
done
popd