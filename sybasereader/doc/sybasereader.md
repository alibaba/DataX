
# SybaseReader 插件文档


___


## 1 快速介绍

SybaseReader插件实现了从Sybase读取数据。在底层实现上，SybaseReader通过JDBC连接远程Sybase数据库，并执行相应的sql语句将数据从Sybase库中SELECT出来。

## 2 实现原理

简而言之，SybaseReader通过JDBC连接器连接到远程的Sybase数据库，并根据用户配置的信息生成查询SELECT SQL语句并发送到远程Sybase数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column、Where的信息，SybaseReader将其拼接为SQL语句发送到Sybase数据库；对于用户配置querySql信息，SybaseReader直接将其发送到Sybase数据库。


## 3 功能说明

### 3.1 配置样例

* 配置一个从Sybase数据库同步抽取数据到本地的作业:

```
{
    "job": {
        "setting": {
            "speed": {
            //设置传输速度 byte/s 尽量逼近这个速度但是不高于它.
            // channel 表示通道数量，byte表示通道速度，如果单通道速度1MB，配置byte为1048576表示一个channel
                 "byte": 1048576
            },
            //出错限制
                "errorLimit": {
                //先选择record
                "record": 0,
                //百分比  1表示100%
                "percentage": 0.02
            }
        },
        "content": [
            {
                "reader": {
                    "name": "sybasereader",
                    "parameter": {
                        // 数据库连接用户名
                        "username": "root",
                        // 数据库连接密码
                        "password": "root",
                        "column": [
                            "id","name"
                        ],
                        //切分主键
                        "splitPk": "db_id",
                        "connection": [
                            {
                                "table": [
                                    "table"
                                ],
                                "jdbcUrl": [
     "jdbc:sybase:Tds:[HOST_NAME]:PORT/[DATABASE_NAME]"
                                ]
                            }
                        ]
                    }
                },
               "writer": {
                  //writer类型
                    "name": "streamwriter",
                  // 是否打印内容
                    "parameter": {
                        "print": true
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
            	"channel": 5
            }
        },
        "content": [
            {
                "reader": {
                    "name": "sybasereader",
                    "parameter": {
                        "username": "root",
                        "password": "root",
                        "where": "",
                        "connection": [
                            {
                                "querySql": [
                                    "select db_id,on_line_flag from db_info where db_id < 10;"
                                ],
                                "jdbcUrl": [
                                    "jdbc:sybase:Tds:[HOST_NAME]:PORT/[DATABASE_NAME]"
                                ]
                            }
                        ]
                    }
                },
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "visible": false,
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

    * 描述：描述的是到对端数据库的JDBC连接信息，使用JSON的数组描述，并支持一个库填写多个连接地址。之所以使用JSON数组描述连接信息，是因为阿里集团内部支持多个IP探测，如果配置了多个，SybaseReader可以依次探测ip的可连接性，直到选择一个合法的IP。如果全部连接失败，SybaseReader报错。 注意，jdbcUrl必须包含在connection配置单元中。

		jdbcUrl按照Sybase官方规范，并可以填写连接附件控制信息。

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

	* 描述：所选取的需要同步的表。使用JSON的数组描述，因此支持多张表同时抽取。当配置为多张表时，用户自己需保证多张表是同一schema结构，SybaseReader不予检查表是否同一逻辑表。注意，table必须包含在connection配置单元中。<br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：所配置的表中需要同步的列名集合，使用JSON的数组描述字段信息。用户使用\*代表默认使用所有列配置，例如['\*']。

	  支持列裁剪，即列可以挑选部分列进行导出。

      支持列换序，即列可以不按照表schema信息进行导出。

	  支持常量配置，用户需要按照JSON格式:
	  ["id", "`table`", "1", "'bazhen.csy'", "null", "to_char(a + 1)", "2.3" , "true"]
	  id为普通列名，\`table\`为包含保留在的列名，1为整形数字常量，'bazhen.csy'为字符串常量，null为空指针，to_char(a + 1)为表达式，2.3为浮点数，true为布尔值。

		Column必须显示填写，不允许为空！

	* 必选：是 <br />

	* 默认值：无 <br />

* **splitPk**

	* 描述：SybaseReader进行数据抽取时，如果指定splitPk，表示用户希望使用splitPk代表的字段进行数据分片，DataX因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能。

	  推荐splitPk用户使用表主键，因为表主键通常情况下比较均匀，因此切分出来的分片也不容易出现数据热点。

	  目前splitPk仅支持整形、字符串型数据切分，`不支持浮点、日期等其他类型`。如果用户指定其他非支持类型，SybaseReader将报错！

	 splitPk如果不填写，将视作用户不对单表进行切分，SybaseReader使用单通道同步全量数据。

	* 必选：否 <br />

	* 默认值：无 <br />

* **where**

	* 描述：筛选条件，SybaseReader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为gmt_create > $bizdate 。注意：不可以将where条件指定为limit 10，limit不是SQL的合法where子句。<br />

          where条件可以有效地进行业务增量同步。

	* 必选：否 <br />

	* 默认值：无 <br />

* **querySql**

	* 描述：在有些业务场景下，where这一配置项不足以描述所筛选的条件，用户可以通过该配置型来自定义筛选SQL。当用户配置了这一项之后，DataX系统就会忽略table，column这些配置型，直接使用这个配置项的内容对数据进行筛选，例如需要进行多表join后同步数据，使用select a,b from table_a join table_b on table_a.id = table_b.id <br />

	 `当用户配置querySql时，SybaseReader直接忽略table、column、where条件的配置`。

	* 必选：否 <br />

	* 默认值：无 <br />

* **fetchSize**

	* 描述：该配置项定义了插件和数据库服务器端每次批量数据获取条数，该值决定了DataX和服务器端的网络交互次数，能够较大的提升数据抽取性能。<br />

	 `注意，该值过大(>2048)可能造成DataX进程OOM。`。

	* 必选：否 <br />

	* 默认值：1024 <br />




### 3.3 类型转换（此处待测试更新，暂时使用oracle支持类型对应）

目前SybaseReader支持大部分Sybase类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出SybaseReader针对Sybase类型转换列表:


| DataX 内部类型| Sybase 数据类型    |
| -------- | -----  |
| Long     |NUMBER,INTEGER,INT,SMALLINT|
| Double   |NUMERIC,DECIMAL,FLOAT,DOUBLE PRECISION,REAL|
| String   |LONG,CHAR,NCHAR,VARCHAR,VARCHAR2,NVARCHAR2,CLOB,NCLOB,CHARACTER,CHARACTER VARYING,CHAR VARYING,NATIONAL CHARACTER,NATIONAL CHAR,NATIONAL CHARACTER VARYING,NATIONAL CHAR VARYING,NCHAR VARYING    |
| Date     |TIMESTAMP,DATE    |
| Boolean  |bit, bool   |
| Bytes    |BLOB,BFILE,RAW,LONG RAW    |







