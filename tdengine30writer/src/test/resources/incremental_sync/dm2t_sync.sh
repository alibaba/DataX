#!/bin/bash

set -e
#set -x

datax_home_dir=$(dirname $(readlink -f "$0"))
table_name="stb1"
update_key="ts"

while getopts "hd:t:" arg; do
  case $arg in
  d)
    datax_home_dir=$(echo $OPTARG)
    ;;
  v)
    table_name=$(echo $OPTARG)
    ;;
  h)
    echo "Usage: $(basename $0) -d [datax_home_dir] -t [table_name] -k [update_key]"
    echo "                    -h help"
    exit 0
    ;;
  ?) #unknow option
    echo "unkonw argument"
    exit 1
    ;;
  esac
done

if [[ -e ${datax_home_dir}/job/${table_name}.csv ]]; then
  MAX_TIME=$(cat ${datax_home_dir}/job/${table_name}.csv)
else
  MAX_TIME="null"
fi
current_datetime=$(date +"%Y-%m-%d %H:%M:%S")
current_timestamp=$(date +%s)

if [ "$MAX_TIME" != "null" ]; then
  WHERE="${update_key} >= '$MAX_TIME' and ${update_key} < '$current_datetime'"
  sed "s/1=1/$WHERE/g" ${datax_home_dir}/job/dm2t-update.json >${datax_home_dir}/job/dm2t_${current_timestamp}.json
  echo "incremental data synchronization, from '${MAX_TIME}' to '${current_datetime}'"
  python ${datax_home_dir}/bin/datax.py ${datax_home_dir}/job/dm2t_${current_timestamp}.json 1> /dev/null 2>&1
else
  echo "full data synchronization, to '${current_datetime}'"
  python ${datax_home_dir}/bin/datax.py ${datax_home_dir}/job/dm2t-update.json 1> /dev/null 2>&1
fi

if [[ $? -ne 0 ]]; then
  echo "datax migration job falied"
else
  echo ${current_datetime} >$datax_home_dir/job/${table_name}.csv
  echo "datax migration job success"
fi

rm -rf ${datax_home_dir}/job/dm2t_${current_timestamp}.json

#while true; do ./dm2t_sync.sh; sleep 5s; done