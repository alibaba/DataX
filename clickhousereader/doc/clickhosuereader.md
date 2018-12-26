
# ClickhouseReader 插件文档


___



## 1 快速介绍

ClickhouseReader插件实现了从Clickhouse读取数据。在底层实现上，ClickhouseReader通过JDBC连接远程Clickhouse数据库，并执行相应的sql语句将数据从mysql库中SELECT出来。

**不同于其他关系型数据库，CLickhouseReader不支持FetchSize.**

## 2 实现原理

简而言之，ClickhouseReader通过JDBC连接器连接到远程的Clickhouse数据库，并根据用户配置的信息生成查询SELECT SQL语句，然后发送到远程Clickhouse数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column、Where的信息，ClickhouseReader将其拼接为SQL语句发送到Clickhouse数据库；对于用户配置querySql信息，ClickhouseReader直接将其发送到Clickhouse数据库。


## 3 功能说明

### 3.1 配置样例

* 配置一个从Clickhouse数据库同步抽取数据到本地的作业:

```
{
    "job": {
        "setting": {
            "speed": {
                 "channel": 3
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0.02
            }
        },
        "content": [
            {
                "reader": {
                    "name": "clickhousereader",
                    "parameter": {
                        "username": "root",
                        "password": "root",
                        "column": [
                            "id",
                            "name"
                        ],
                        "splitPk": "db_id",
                        "connection": [
                            {
                                "table": [
                                    "table"
                                ],
                                "jdbcUrl": [
     "jdbc:mysql://127.0.0.1:3306/database"
                                ]
                            }
                        ]
                    }
                },
               "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print":true
                    }
                }
            }
        ]
    }
}

```

* 配置一个自定义SQL的数据库同步任务到本地内容的作业：

```
{
    "job": {
        "setting": {
            "speed": {
                 "channel":1
            }
        },
        "content": [
            {
                "reader": {
                    "name": "clickhousereader",
                    "parameter": {
                        "username": "root",
                        "password": "root",
                        "connection": [
                            {
                                "querySql": [
                                    "select db_id,on_line_flag from db_info where db_id < 10;"
                                ],
                                "jdbcUrl": [
                                    "jdbc:mysql://bad_ip:3306/database",
                                    "jdbc:mysql://127.0.0.1:bad_port/database",
                                    "jdbc:mysql://127.0.0.1:3306/database"
                                ]
                            }
                        ]
                    }
                },
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print": false,
                        "encoding": "UTF-8"
                    }
                }
            }
        ]
    }
}
```


### 3.2 参数说明

* **jdbcUrl**

	* 描述：描述的是到对端数据库的JDBC连接信息，使用JSON的数组描述，并支持一个库填写多个连接地址。之所以使用JSON数组描述连接信息，是因为阿里集团内部支持多个IP探测，如果配置了多个，ClickhouseReader可以依次探测ip的可连接性，直到选择一个合法的IP。如果全部连接失败，ClickhouseReader报错。 注意，jdbcUrl必须包含在connection配置单元中。对于阿里集团外部使用情况，JSON数组填写一个JDBC连接即可。

		jdbcUrl按照Clickhouse官方规范，并可以填写连接附件控制信息。具体请参看[Clickhouse官方文档](https://clickhouse.yandex/docs/en/interfaces/jdbc/)。

	* 必选：是 <br />

	* 默认值：无 <br />

* **username**

	* 描述：数据源的用户名 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **password**

	* 描述：数据源指定用户名的密码 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **table**

	* 描述：所选取的需要同步的表。使用JSON的数组描述，因此支持多张表同时抽取。当配置为多张表时，用户自己需保证多张表是同一schema结构，ClickhouseReader不予检查表是否同一逻辑表。注意，table必须包含在connection配置单元中。<br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：所配置的表中需要同步的列名集合，使用JSON的数组描述字段信息。用户使用\*代表默认使用所有列配置，例如['\*']。

	  支持列裁剪，即列可以挑选部分列进行导出。

      支持列换序，即列可以不按照表schema信息进行导出。

	  支持常量配置，用户需要按照Clickhouse SQL语法格式:
	  ["id", "\`table\`", "1", "'bazhen.csy'", "null", "to_char(a + 1)", "2.3" , "true"]
	  id为普通列名，\`table\`为包含保留在的列名，1为整形数字常量，'bazhen.csy'为字符串常量，null为空指针，to_char(a + 1)为表达式，2.3为浮点数，true为布尔值。

	* 必选：是 <br />

	* 默认值：无 <br />

* **splitPk**

	* 描述：ClickhouseReader进行数据抽取时，如果指定splitPk，表示用户希望使用splitPk代表的字段进行数据分片，DataX因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能。

	  推荐splitPk用户使用表主键，因为表主键通常情况下比较均匀，因此切分出来的分片也不容易出现数据热点。

	  目前splitPk仅支持整形数据切分，`不支持浮点、字符串、日期等其他类型`。如果用户指定其他非支持类型，ClickhouseReader将报错！

	  如果splitPk不填写，包括不提供splitPk或者splitPk值为空，DataX视作使用单通道同步该表数据。

	* 必选：否 <br />

	* 默认值：空 <br />

* **where**

	* 描述：筛选条件，ClickhouseReader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为gmt_create > $bizdate 。注意：不可以将where条件指定为limit 10，limit不是SQL的合法where子句。<br />

          where条件可以有效地进行业务增量同步。如果不填写where语句，包括不提供where的key或者value，DataX均视作同步全量数据。

	* 必选：否 <br />

	* 默认值：无 <br />

* **querySql**

	* 描述：在有些业务场景下，where这一配置项不足以描述所筛选的条件，用户可以通过该配置型来自定义筛选SQL。当用户配置了这一项之后，DataX系统就会忽略table，column这些配置型，直接使用这个配置项的内容对数据进行筛选，例如需要进行多表join后同步数据，使用select a,b from table_a join table_b on table_a.id = table_b.id <br />

	 `当用户配置querySql时，ClickhouseReader直接忽略table、column、where条件的配置`，querySql优先级大于table、column、where选项。

	* 必选：否 <br />

	* 默认值：无 <br />


### 3.3 类型转换

目前ClickhouseReader支持大部分Clickhouse类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出ClickhouseReader针对Clickhouse类型转换列表:


| DataX 内部类型| Clickhouse 数据类型    |
| -------- | -----  |
| Long     |Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64 |
| Double   |Float32, Float64, Decimal |
| String   |String, FixedString |
| Date     |Date, Datetime |
| Boolean  |Int8 |
| Bytes    |String |



请注意:

* `除上述罗列字段类型外，其他类型均不支持`。

## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征
建表语句：

``` sql
    CREATE TABLE default.test_data_type (
        fd_int8 Int8,
        fd_int16 Int16,
        fd_int32 Int32,
        fd_int64 Int64,
        fd_uint8 UInt8,
        fd_uint16 UInt16,
        fd_uint32 UInt32,
        fd_uint64 UInt64,
        fd_float32 Float32,
        fd_float64 Float64,
        fd_decimal Decimal(14,4),
        fd_string String,
        fd_fixedstring_16 FixedString(16),
        fd_date Date,
        fd_datetime DateTime
    ) ENGINE = ReplacingMergeTree(fd_date, fd_int8, 8192);
```

单行记录类似于：
```
    fd_int8:           -1
    fd_int16:          -1
    fd_int32:          -1
    fd_int64:          -1
    fd_uint8:          1
    fd_uint16:         10
    fd_uint32:         100
    fd_uint64:         100
    fd_float32:        1.1
    fd_float64:        1.1
    fd_decimal:        0.1111
    fd_string:         string
    fd_fixedstring_16: fixed string
    fd_date:           2011-11-11
    fd_datetime:       2011-11-11 11:11:11
```

#### 4.1.2 机器参数

* 执行DataX的机器参数为:
	1. cpu: 4核 Intel(R) Xeon(R) CPU E5-2630 0 @ 2.30GHz
	2. mem: 32GB
	3. net: 百兆
	4. disc: DataX 数据不落磁盘，不统计此项

* Clickhouse数据库机器参数为:
	1. cpu: 4核 Intel(R) Xeon(R) CPU E5-2650 v2 @ 2.60GHz
	2. mem: 32GB
	3. net: 百兆
	4. disc: 高效云盘

#### 4.1.3 DataX jvm 参数

	-Xms1024m -Xmx1024m -XX:+HeapDumpOnOutOfMemoryError


## 5 约束限制


## 6 FAQ

***

**Q: ClickhouseReader同步报错，报错信息为XXX**

 A: 网络或者权限问题，请使用mysql命令行测试：

    clickhouse-client --user <username> --password <password> --host <ip> --database <database> --query "select * from <表名>"

如果上述命令也报错，那可以证实是环境问题，请联系你的DBA。


