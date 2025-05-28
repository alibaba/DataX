select tablespace_name
from dba_data_files;

create
tablespace test datafile '/home/dmdba/dmdbms/data/DAMENG/test.dbf' size 32 autoextend on next 1 maxsize 1024;

create
user TESTUSER identified by test123456 default tablespace test;

grant dba to TESTUSER;

select *
from user_tables;

drop table if exists stb1;

create table stb1
(
    ts  timestamp,
    f1  tinyint,
    f2  smallint,
    f3  int,
    f4  bigint,
    f5  float,
    f6  double,
    f7  NUMERIC(10, 2),
    f8  BIT,
    f9  VARCHAR(100),
    f10 VARCHAR2(200)
);
