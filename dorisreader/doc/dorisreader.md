# DorisReader 插件文档

___

## 1 快速介绍

DorisReader插件实现了从Doris读取数据。在底层实现上，DorisReader通过JDBC连接远程Doris数据库，并执行相应的sql语句将数据从doris库中SELECT出来。

## 2 实现原理

简而言之，DorisReader通过JDBC连接器连接到远程的Doris数据库，并根据用户配置的信息生成查询SELECT
SQL语句，然后发送到远程Doris数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column、Where的信息，DorisReader将其拼接为SQL语句发送到Doris数据库；对于用户配置querySql信息，DorisReader直接将其发送到Doris数据库。

## 3 功能说明

### 3.1 配置样例

* 配置一个从Doris数据库同步抽取数据到本地的作业:

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
                    "name": "dorisreader",
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
     "jdbc:Doris://127.0.0.1:9030/database"
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
                    "name": "dorisreader",
                    "parameter": {
                        "username": "root",
                        "password": "root",
                        "connection": [
                            {
                                "querySql": [
                                    "select db_id,on_line_flag from db_info where db_id < 10;",
                                    "select db_id,on_line_flag from db_info where db_id >= 10;"

                                ],
                                "jdbcUrl": [
                                    "jdbc:Doris://127.0.0.1:9030/database"
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

    *
    描述：描述的是到对端数据库的JDBC连接信息，使用JSON的数组描述，并支持一个库填写多个连接地址。之所以使用JSON数组描述连接信息，是因为阿里集团内部支持多个IP探测，如果配置了多个，DorisReader可以依次探测ip的可连接性，直到选择一个合法的IP。如果全部连接失败，DorisReader报错。
    注意，jdbcUrl必须包含在connection配置单元中。对于阿里集团外部使用情况，JSON数组填写一个JDBC连接即可。

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

    *
    描述：所选取的需要同步的表。使用JSON的数组描述，因此支持多张表同时抽取。当配置为多张表时，用户自己需保证多张表是同一schema结构，DorisReader不予检查表是否同一逻辑表。注意，table必须包含在connection配置单元中。<br />

    * 必选：是 <br />

    * 默认值：无 <br />

* **column**

    * 描述：所配置的表中需要同步的列名集合，使用JSON的数组描述字段信息。用户使用\*代表默认使用所有列配置，例如['\*']。

      支持列裁剪，即列可以挑选部分列进行导出。

      支持列换序，即列可以不按照表schema信息进行导出。

      支持常量配置，用户需要按照Doris SQL语法格式:
      ["id", "\`table\`", "1", "'bazhen.csy'", "null", "to_char(a + 1)", "2.3" , "true"]
      id为普通列名，\`table\`为包含保留字的列名，1为整形数字常量，'bazhen.csy'为字符串常量，null为空指针，to_char(a + 1)为表达式，2.3为浮点数，true为布尔值。

    * 必选：是 <br />

    * 默认值：无 <br />

* **splitPk**

    * 描述：DorisReader进行数据抽取时，如果指定splitPk，表示用户希望使用splitPk代表的字段进行数据分片，DataX因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能。

      推荐splitPk用户使用表主键，因为表主键通常情况下比较均匀，因此切分出来的分片也不容易出现数据热点。

  目前splitPk仅支持整形数据切分，`不支持浮点、字符串、日期等其他类型`。如果用户指定其他非支持类型，DorisReader将报错！

  如果splitPk不填写，包括不提供splitPk或者splitPk值为空，DataX视作使用单通道同步该表数据。

    * 必选：否 <br />

    * 默认值：空 <br />

* **where**

    * 描述：筛选条件，DorisReader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为gmt_create >
      $bizdate 。注意：不可以将where条件指定为limit 10，limit不是SQL的合法where子句。<br />

          where条件可以有效地进行业务增量同步。如果不填写where语句，包括不提供where的key或者value，DataX均视作同步全量数据。

    * 必选：否 <br />

    * 默认值：无 <br />

* **querySql**

    *
    描述：在有些业务场景下，where这一配置项不足以描述所筛选的条件，用户可以通过该配置型来自定义筛选SQL。当用户配置了这一项之后，DataX系统就会忽略table，column这些配置型，直接使用这个配置项的内容对数据进行筛选，例如需要进行多表join后同步数据，使用select
    a,b from table_a join table_b on table_a.id = table_b.id <br />

  `当用户配置querySql时，DorisReader直接忽略table、column、where条件的配置`，querySql优先级大于table、column、where选项。

    * 必选：否 <br />

    * 默认值：无 <br />

### 3.3 类型转换

目前DorisReader支持大部分Doris类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出DorisReaderr针对Doris类型转换列表:

| DataX 内部类型| doris 数据类型                                            |
| -------- |-------------------------------------------------------|
| Long     | int, tinyint, smallint, int, bigint,Largint           |
| Double   | float, double, decimal                                |
| String   | varchar, char, text, string, map, json, array, struct |
| Date     | date, datetime                                        |
| Boolean  | Boolean                                               |

请注意:

* `tinyint(1) DataX视作为整形`。



