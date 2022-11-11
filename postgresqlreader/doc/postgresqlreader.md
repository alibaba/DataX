
# PostgresqlReader 插件文档


___


## 1 快速介绍

PostgresqlReader插件实现了从PostgreSQL读取数据。在底层实现上，PostgresqlReader通过JDBC连接远程PostgreSQL数据库，并执行相应的sql语句将数据从PostgreSQL库中SELECT出来。

## 2 实现原理

简而言之，PostgresqlReader通过JDBC连接器连接到远程的PostgreSQL数据库，并根据用户配置的信息生成查询SELECT SQL语句并发送到远程PostgreSQL数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column、Where的信息，PostgresqlReader将其拼接为SQL语句发送到PostgreSQL数据库；对于用户配置querySql信息，PostgresqlReader直接将其发送到PostgreSQL数据库。


## 3 功能说明

### 3.1 配置样例

* 配置一个从PostgreSQL数据库同步抽取数据到本地的作业:

```
{
    "job": {
        "setting": {
            "speed": {
            //设置传输速度，单位为byte/s，DataX运行会尽可能达到该速度但是不超过它.
                 "byte": 1048576
            },
            //出错限制
                "errorLimit": {
                //出错的record条数上限，当大于该值即报错。
                "record": 0,
                //出错的record百分比上限 1.0表示100%，0.02表示2%
                "percentage": 0.02
            }
        },
        "content": [
            {
                "reader": {
                    "name": "postgresqlreader",
                    "parameter": {
                        // 数据库连接用户名
                        "username": "xx",
                        // 数据库连接密码
                        "password": "xx",
                        "column": [
                            "id","name"
                        ],
                        //切分主键
                        "splitPk": "id",
                        "connection": [
                            {
                                "table": [
                                    "table"
                                ],
                                "jdbcUrl": [
     "jdbc:postgresql://host:port/database"
                                ]
                            }
                        ]
                    }
                },
               "writer": {
                    //writer类型
                    "name": "streamwriter",
                    //是否打印内容
                    "parameter": {
                        "print":true,
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
            "speed": 1048576
        },
        "content": [
            {
                "reader": {
                    "name": "postgresqlreader",
                    "parameter": {
                        "username": "xx",
                        "password": "xx",
                        "where": "",
                        "connection": [
                            {
                                "querySql": [
                                    "select db_id,on_line_flag from db_info where db_id < 10;"
                                ],
                                "jdbcUrl": [
                                    "jdbc:postgresql://host:port/database", "jdbc:postgresql://host:port/database"
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

	* 描述：描述的是到对端数据库的JDBC连接信息，使用JSON的数组描述，并支持一个库填写多个连接地址。之所以使用JSON数组描述连接信息，是因为阿里集团内部支持多个IP探测，如果配置了多个，PostgresqlReader可以依次探测ip的可连接性，直到选择一个合法的IP。如果全部连接失败，PostgresqlReader报错。 注意，jdbcUrl必须包含在connection配置单元中。对于阿里集团外部使用情况，JSON数组填写一个JDBC连接即可。

		jdbcUrl按照PostgreSQL官方规范，并可以填写连接附件控制信息。具体请参看[PostgreSQL官方文档](http://jdbc.postgresql.org/documentation/93/connect.html)。

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

	* 描述：所选取的需要同步的表。使用JSON的数组描述，因此支持多张表同时抽取。当配置为多张表时，用户自己需保证多张表是同一schema结构，PostgresqlReader不予检查表是否同一逻辑表。注意，table必须包含在connection配置单元中。<br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：所配置的表中需要同步的列名集合，使用JSON的数组描述字段信息。用户使用\*代表默认使用所有列配置，例如['\*']。

	  支持列裁剪，即列可以挑选部分列进行导出。

      支持列换序，即列可以不按照表schema信息进行导出。

	  支持常量配置，用户需要按照PostgreSQL语法格式:
	  ["id", "'hello'::varchar", "true", "2.5::real", "power(2,3)"]
	  id为普通列名，'hello'::varchar为字符串常量，true为布尔值，2.5为浮点数, power(2,3)为函数。

		**column必须用户显示指定同步的列集合，不允许为空！**

	* 必选：是 <br />

	* 默认值：无 <br />

* **splitPk**

	* 描述：PostgresqlReader进行数据抽取时，如果指定splitPk，表示用户希望使用splitPk代表的字段进行数据分片，DataX因此会启动并发任务进行数据同步，这样可以大大提高数据同步的效能。

	  推荐splitPk用户使用表主键，因为表主键通常情况下比较均匀，因此切分出来的分片也不容易出现数据热点。

	  目前splitPk仅支持整形数据切分，`不支持浮点、字符串型、日期等其他类型`。如果用户指定其他非支持类型，PostgresqlReader将报错！

	 	splitPk设置为空，底层将视作用户不允许对单表进行切分，因此使用单通道进行抽取。

	* 必选：否 <br />

	* 默认值：空 <br />

* **where**

	* 描述：筛选条件，MysqlReader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为gmt_create > $bizdate 。注意：不可以将where条件指定为limit 10，limit不是SQL的合法where子句。<br />

          where条件可以有效地进行业务增量同步。		where条件不配置或者为空，视作全表同步数据。

	* 必选：否 <br />

	* 默认值：无 <br />

* **querySql**

	* 描述：在有些业务场景下，where这一配置项不足以描述所筛选的条件，用户可以通过该配置型来自定义筛选SQL。当用户配置了这一项之后，DataX系统就会忽略table，column这些配置型，直接使用这个配置项的内容对数据进行筛选，例如需要进行多表join后同步数据，使用select a,b from table_a join table_b on table_a.id = table_b.id <br />

	 `当用户配置querySql时，PostgresqlReader直接忽略table、column、where条件的配置`。

	* 必选：否 <br />

	* 默认值：无 <br />

* **fetchSize**

	* 描述：该配置项定义了插件和数据库服务器端每次批量数据获取条数，该值决定了DataX和服务器端的网络交互次数，能够较大的提升数据抽取性能。<br />

	 `注意，该值过大(>2048)可能造成DataX进程OOM。`。

	* 必选：否 <br />

	* 默认值：1024 <br />


### 3.3 类型转换

目前PostgresqlReader支持大部分PostgreSQL类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出PostgresqlReader针对PostgreSQL类型转换列表:


| DataX 内部类型| PostgreSQL 数据类型    |
| -------- | -----  |
| Long     |bigint, bigserial, integer, smallint, serial |
| Double   |double precision, money, numeric, real |
| String   |varchar, char, text, bit, inet|
| Date     |date, time, timestamp |
| Boolean  |bool|
| Bytes    |bytea|

请注意:

* `除上述罗列字段类型外，其他类型均不支持; money,inet,bit需用户使用a_inet::varchar类似的语法转换`。

## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征
建表语句：

create table pref_test(
     id serial,
     a_bigint bigint,
     a_bit bit(10),
     a_boolean boolean,
     a_char character(5),
     a_date date,
     a_double double precision,
     a_integer integer,
     a_money money,
     a_num numeric(10,2),
     a_real real,
     a_smallint smallint,
     a_text text,
     a_time time,
     a_timestamp timestamp
)

#### 4.1.2 机器参数

* 执行DataX的机器参数为:
	1. cpu: 16核 Intel(R) Xeon(R) CPU E5620  @ 2.40GHz
	2. mem: MemTotal: 24676836kB    MemFree: 6365080kB
	3. net: 百兆双网卡

* PostgreSQL数据库机器参数为:
	D12 24逻辑核  192G内存 12*480G SSD 阵列


### 4.2 测试报告

#### 4.2.1 单表测试报告


| 通道数 | 是否按照主键切分 | DataX速度(Rec/s) | DataX流量(MB/s) | DataX机器运行负载 |
|--------|--------| --------|--------|--------|
|1| 否 | 10211 | 0.63 | 0.2 |
|1| 是 | 10211 | 0.63 | 0.2 |
|4| 否 | 10211 | 0.63 | 0.2 |
|4| 是 | 40000 | 2.48 | 0.5 |
|8| 否 | 10211 | 0.63 | 0.2 |
|8| 是 | 78048 | 4.84 | 0.8 |


说明：

1. 这里的单表，主键类型为 serial，数据分布均匀。
2. 对单表如果没有按照主键切分，那么配置通道个数不会提升速度，效果与1个通道一样。
