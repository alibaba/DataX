# DataX OracleWriter


---


## 1 快速介绍

OracleWriter 插件实现了写入数据到 Oracle 主库的目的表的功能。在底层实现上， OracleWriter 通过 JDBC 连接远程 Oracle 数据库，并执行相应的 insert into ...  sql 语句将数据写入 Oracle，内部会分批次提交入库。

OracleWriter 面向ETL开发工程师，他们使用 OracleWriter 从数仓导入数据到 Oracle。同时 OracleWriter 亦可以作为数据迁移工具为DBA等用户提供服务。


## 2 实现原理

OracleWriter 通过 DataX 框架获取 Reader 生成的协议数据，根据你配置生成相应的SQL语句


* `insert into...`(当主键/唯一性索引冲突时会写不进去冲突的行)

<br />

    注意：
    1. 目的表所在数据库必须是主库才能写入数据；整个任务至少需具备 insert into...的权限，是否需要其他权限，取决于你任务配置中在 preSql 和 postSql 中指定的语句。
    2.OracleWriter和MysqlWriter不同，不支持配置writeMode参数。


## 3 功能说明

### 3.1 配置样例

* 这里使用一份从内存产生到 Oracle 导入的数据。

```json
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "streamreader",
                    "parameter": {
                        "column" : [
                            {
                                "value": "DataX",
                                "type": "string"
                            },
                            {
                                "value": 19880808,
                                "type": "long"
                            },
                            {
                                "value": "1988-08-08 08:08:08",
                                "type": "date"
                            },
                            {
                                "value": true,
                                "type": "bool"
                            },
                            {
                                "value": "test",
                                "type": "bytes"
                            }
                        ],
                        "sliceRecordCount": 1000
                    }
                },
                "writer": {
                    "name": "oraclewriter",
                    "parameter": {
                        "username": "root",
                        "password": "root",
                        "column": [
                            "id",
                            "name"
                        ],
                        "preSql": [
                            "delete from test"
                        ],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:oracle:thin:@[HOST_NAME]:PORT:[DATABASE_NAME]",
                                "table": [
                                    "test"
                                ]
                            }
                        ]
                    }
                }
            }
        ]
    }
}

```


### 3.2 参数说明

* **jdbcUrl**

    * 描述：目的数据库的 JDBC 连接信息 ,jdbcUrl必须包含在connection配置单元中。

               注意：1、在一个数据库上只能配置一个值。这与 OracleReader 支持多个备库探测不同，因为此处不支持同一个数据库存在多个主库的情况(双主导入数据情况)
                    2、jdbcUrl按照Oracle官方规范，并可以填写连接附加参数信息。具体请参看 Oracle官方文档或者咨询对应 DBA。


  * 必选：是 <br />

  * 默认值：无 <br />

* **username**

  * 描述：目的数据库的用户名 <br />

  * 必选：是 <br />

  * 默认值：无 <br />

* **password**

  * 描述：目的数据库的密码 <br />

  * 必选：是 <br />

  * 默认值：无 <br />

* **table**

  * 描述：目的表的表名称。支持写入一个或者多个表。当配置为多张表时，必须确保所有表结构保持一致。

               注意：table 和 jdbcUrl 必须包含在 connection 配置单元中

  * 必选：是 <br />

  * 默认值：无 <br />

* **column**

  * 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。如果要依次写入全部列，使用*表示, 例如: "column": ["*"]

    		**column配置项必须指定，不能留空！**


               注意：1、我们强烈不推荐你这样配置，因为当你目的表字段个数、类型等有改动时，你的任务可能运行不正确或者失败
                    2、此处 column 不能配置任何常量值

  * 必选：是 <br />

  * 默认值：否 <br />

* **preSql**

  * 描述：写入数据到目的表前，会先执行这里的标准语句。如果 Sql 中有你需要操作到的表名称，请使用 `@table` 表示，这样在实际执行 Sql 语句时，会对变量按照实际表名称进行替换。比如你的任务是要写入到目的端的100个同构分表(表名称为:datax_00,datax01, ... datax_98,datax_99)，并且你希望导入数据前，先对表中数据进行删除操作，那么你可以这样配置：`"preSql":["delete from @table"]`，效果是：在执行到每个表写入数据前，会先执行对应的 delete from 对应表名称 <br />

  * 必选：否 <br />

  * 默认值：无 <br />

* **postSql**

  * 描述：写入数据到目的表后，会执行这里的标准语句。（原理同 preSql ） <br />

  * 必选：否 <br />

  * 默认值：无 <br />

* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与Oracle的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。<br />

	* 必选：否 <br />

	* 默认值：1024 <br />

* **session**

    * 描述：设置oracle连接时的session信息，格式示例如下：<br />

    ```
    "session":[
                "alter session set nls_date_format = 'dd.mm.yyyy hh24:mi:ss';"
                "alter session set NLS_LANG = 'AMERICAN';"
    ]

    ```

	* 必选：否 <br />

	* 默认值：无 <br />

### 3.3 类型转换

类似 OracleReader ，目前 OracleWriter 支持大部分 Oracle 类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出 OracleWriter 针对 Oracle 类型转换列表:


| DataX 内部类型| Oracle 数据类型    |
| -------- | -----  |
| Long     |NUMBER,INTEGER,INT,SMALLINT|
| Double   |NUMERIC,DECIMAL,FLOAT,DOUBLE PRECISION,REAL|
| String   |LONG,CHAR,NCHAR,VARCHAR,VARCHAR2,NVARCHAR2,CLOB,NCLOB,CHARACTER,CHARACTER VARYING,CHAR VARYING,NATIONAL CHARACTER,NATIONAL CHAR,NATIONAL CHARACTER VARYING,NATIONAL CHAR VARYING,NCHAR VARYING    |
| Date     |TIMESTAMP,DATE    |
| Boolean  |bit, bool   |
| Bytes    |BLOB,BFILE,RAW,LONG RAW    |



## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征
建表语句：
```
--DROP TABLE PERF_ORACLE_WRITER;
CREATE TABLE PERF_ORACLE_WRITER (
COL1 VARCHAR2(255 BYTE) NULL ,
COL2 NUMBER(32) NULL ,
COL3 NUMBER(32) NULL ,
COL4 DATE NULL ,
COL5 FLOAT NULL ,
COL6 VARCHAR2(255 BYTE) NULL ,
COL7 VARCHAR2(255 BYTE) NULL ,
COL8 VARCHAR2(255 BYTE) NULL ,
COL9 VARCHAR2(255 BYTE) NULL ,
COL10 VARCHAR2(255 BYTE) NULL
)
LOGGING
NOCOMPRESS
NOCACHE;
```
单行记录类似于：
```
col1:485924f6ab7f272af361cd3f7f2d23e0d764942351#$%^&fdafdasfdas%%^(*&^^&*
co12:1
co13:1696248667889
co14:2013-01-06 00:00:00
co15:3.141592653578
co16:100dafdsafdsahofjdpsawifdishaf;dsadsafdsahfdsajf;dsfdsa;fjdsal;11209
co17:100dafdsafdsahofjdpsawifdishaf;dsadsafdsahfdsajf;dsfdsa;fjdsal;11fdsafdsfdsa209
co18:100DAFDSAFDSAHOFJDPSAWIFDISHAF;dsadsafdsahfdsajf;dsfdsa;FJDSAL;11209
co19:100dafdsafdsahofjdpsawifdishaf;DSADSAFDSAHFDSAJF;dsfdsa;fjdsal;11209
co110:12~!2345100dafdsafdsahofjdpsawifdishaf;dsadsafdsahfdsajf;dsfdsa;fjdsal;11209
```
#### 4.1.2 机器参数

* 执行 DataX 的机器参数为:
    1. cpu: 24 Core Intel(R) Xeon(R) CPU E5-2430 0 @ 2.20GHz
    2. mem: 94GB
	3. net: 千兆双网卡
	4. disc: DataX 数据不落磁盘，不统计此项

* Oracle 数据库机器参数为:
    1. cpu: 4 Core Intel(R) Xeon(R) CPU E5420  @ 2.50GHz
    2. mem: 7GB

#### 4.1.3 DataX jvm 参数

    -Xms1024m -Xmx1024m -XX:+HeapDumpOnOutOfMemoryError

#### 4.1.4 性能测试作业配置

```
{
    "job": {
        "setting": {
            "speed": {
                "channel": 4
            }
        },
        "content": [
            {
                "reader": {
                    "name": "streamreader",
                    "parameter": {
                        "sliceRecordCount": 1000000000,
                        "column": [
                            {
                                "value": "485924f6ab7f272af361cd3f7f2d23e0d764942351#$%^&fdafdasfdas%%^(*&^^&*"
                            },
                            {
                                "value": 1,
                                "type": "long"
                            },
                            {
                                "value": "1696248667889",
                                "type": "long"
                            },
                            {
                                "type": "date",
                                "value": "2013-07-06 00:00:00",
                                "dateFormat": "yyyy-mm-dd hh:mm:ss"
                            },
                            {
                                "value": "3.141592653578",
                                "type": "double"
                            },
                            {
                                "value": "100dafdsafdsahofjdpsawifdishaf;dsadsafdsahfdsajf;dsfdsa;fjdsal;11209"
                            },
                            {
                                "value": "100dafdsafdsahofjdpsawifdishaf;dsadsafdsahfdsajf;dsfdsa;fjdsal;11fdsafdsfdsa209"
                            },
                            {
                                "value": "100DAFDSAFDSAHOFJDPSAWIFDISHAF;dsadsafdsahfdsajf;dsfdsa;FJDSAL;11209"
                            },
                            {
                                "value": "100dafdsafdsahofjdpsawifdishaf;DSADSAFDSAHFDSAJF;dsfdsa;fjdsal;11209"
                            },
                            {
                                "value": "12~!2345100dafdsafdsahofjdpsawifdishaf;dsadsafdsahfdsajf;dsfdsa;fjdsal;11209"
                            }
                        ]
                    }
                },
                "writer": {
                    "name": "oraclewriter",
                    "parameter": {
                        "username": "username",
                        "password": "password",
                        "truncate": "true",
                        "batchSize": "512",
                        "column": [
                            "col1",
                            "col2",
                            "col3",
                            "col4",
                            "col5",
                            "col6",
                            "col7",
                            "col8",
                            "col9",
                            "col10"
                        ],
                        "connection": [
                            {
                                "table": [
                                    "PERF_ORACLE_WRITER"
                                ],
                                "jdbcUrl": "jdbc:oracle:thin:@ip:port:dataplat"
                            }
                        ]
                    }
                }
            }
        ]
    }
}

```

### 4.2 测试报告

#### 4.2.1 测试报告

| 通道数|  批量提交行数| DataX速度(Rec/s)|DataX流量(MB/s)| DataX机器网卡流出流量(MB/s)|DataX机器运行负载|DB网卡进入流量(MB/s)|DB运行负载|
|--------|--------| --------|--------|--------|--------|--------|--------|
|1|128|15564|6.51|7.5|0.02|7.4|1.08|
|1|512|29491|10.90|12.6|0.05|12.4|1.55|
|1|1024|31529|11.87|13.5|0.22|13.3|1.58|
|1|2048|33469|12.57|14.3|0.17|14.3|1.53|
|1|4096|31363|12.48|13.4|0.10|10.0|1.72|
|4|10|9440|4.05|5.6|0.01|5.0|3.75|
|4|128|42832|16.48|18.3|0.07|18.5|2.89|
|4|512|46643|20.02|22.7|0.35|21.1|3.31|
|4|1024|39116|16.79|18.7|0.10|18.1|3.05|
|4|2048|39526|16.96|18.5|0.32|17.1|2.86|
|4|4096|37683|16.17|17.2|0.23|15.5|2.26|
|8|128|38336|16.45|17.5|0.13|16.2|3.87|
|8|512|31078|13.34|14.9|0.11|13.4|2.09|
|8|1024|37888|16.26|18.5|0.20|18.5|3.14|
|8|2048|38502|16.52|18.5|0.18|18.5|2.96|
|8|4096|38092|16.35|18.3|0.10|17.8|3.19|
|16|128|35366|15.18|16.9|0.13|15.6|3.49|
|16|512|35584|15.27|16.8|0.23|17.4|3.05|
|16|1024|38297|16.44|17.5|0.20|17.0|3.42|
|16|2048|28467|12.22|13.8|0.10|12.4|3.38|
|16|4096|27852|11.95|12.3|0.11|12.3|3.86|
|32|1024|34406|14.77|15.4|0.09|15.4|3.55|


1. `batchSize 和 通道个数，对性能影响较大`
2. `通常不建议写入数据库时，通道个数 >32`



## 5 约束限制




## FAQ

***

**Q: OracleWriter 执行 postSql 语句报错，那么数据导入到目标数据库了吗?**

A: DataX 导入过程存在三块逻辑，pre 操作、导入操作、post 操作，其中任意一环报错，DataX 作业报错。由于 DataX 不能保证在同一个事务完成上述几个操作，因此有可能数据已经落入到目标端。

***

**Q: 按照上述说法，那么有部分脏数据导入数据库，如果影响到线上数据库怎么办?**

A: 目前有两种解法，第一种配置 pre 语句，该 sql 可以清理当天导入数据， DataX 每次导入时候可以把上次清理干净并导入完整数据。第二种，向临时表导入数据，完成后再 rename 到线上表。

***

**Q: 上面第二种方法可以避免对线上数据造成影响，那我具体怎样操作?**

A: 可以配置临时表导入
