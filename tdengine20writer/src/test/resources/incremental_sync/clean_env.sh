#!/bin/bash

datax_home_dir=$(dirname $(readlink -f "$0"))

curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'drop table if exists db2.stb2;' 192.168.1.93:6041/rest/sql
curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'create table if not exists db2.stb2 (`ts` TIMESTAMP,`f2` SMALLINT,`f4` BIGINT,`f5` FLOAT,`f6` DOUBLE,`f7` DOUBLE,`f8` BOOL,`f9` NCHAR(100),`f10` NCHAR(200)) TAGS (`f1` TINYINT,`f3` INT);' 192.168.1.93:6041/rest/sql

rm -f ${datax_home_dir}/log/*
rm -f ${datax_home_dir}/job/*.csv