
# SqlServerReader 插件文档

___


## 1 快速介绍

SqlServerReader插件实现了从SqlServer读取数据。在底层实现上，SqlServerReader通过JDBC连接远程SqlServer数据库，并执行相应的sql语句将数据从SqlServer库中SELECT出来。

## 2 实现原理

简而言之，SqlServerReader通过JDBC连接器连接到远程的SqlServer数据库，并根据用户配置的信息生成查询SELECT SQL语句并发送到远程SqlServer数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column、Where的信息，SqlServerReader将其拼接为SQL语句发送到SqlServer数据库；对于用户配置querySql信息，SqlServer直接将其发送到SqlServer数据库。


## 3 功能说明

### 3.1 配置样例

* 配置一个从SqlServer数据库同步抽取数据到本地的作业:

```
{
    "job": {
        "setting": {
            "speed": {
                 "byte": 1048576
            }
        },
        "content": [
            {
                "reader": {
                    "name": "sqlserverreader",
                    "parameter": {
                        // 数据库连接用户名
                        "username": "root",
                        // 数据库连接密码
                        "password": "root",
                        "column": [
                            "id"
                        ],
                        "splitPk": "db_id",
                        "connection": [
                            {
                                "table": [
                                    "table"
                                ],
                                "jdbcUrl": [
                                "jdbc:sqlserver://localhost:3433;DatabaseName=dbname"
                                ]
                            }
                        ]
                    }
                },
               "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print": true,
                        "encoding": "UTF-8"
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
                    "name": "sqlserverreader",
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
                                    "jdbc:sqlserver://bad_ip:3433;DatabaseName=dbname",
                                    "jdbc:sqlserver://127.0.0.1:bad_port;DatabaseName=dbname",
                                    "jdbc:sqlserver://127.0.0.1:3306;DatabaseName=dbname"
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

	* 描述：描述的是到对端数据库的JDBC连接信息，使用JSON的数组描述，并支持一个库填写多个连接地址。之所以使用JSON数组描述连接信息，是因为阿里集团内部支持多个IP探测，如果配置了多个，SqlServerReader可以依次探测ip的可连接性，直到选择一个合法的IP。如果全部连接失败，SqlServerReader报错。 注意，jdbcUrl必须包含在connection配置单元中。对于阿里集团外部使用情况，JSON数组填写一个JDBC连接即可。

		jdbcUrl按照SqlServer官方规范，并可以填写连接附件控制信息。具体请参看[SqlServer官方文档](http://technet.microsoft.com/zh-cn/library/ms378749(v=SQL.110).aspx)。

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

	* 描述：所选取的需要同步的表。使用JSON的数组描述，因此支持多张表同时抽取。当配置为多张表时，用户自己需保证多张表是同一schema结构，SqlServerReader不予检查表是否同一逻辑表。注意，table必须包含在connection配置单元中。<br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：所配置的表中需要同步的列名集合，使用JSON的数组描述字段信息。用户使用\*代表默认使用所有列配置，例如["\*"]。

	  支持列裁剪，即列可以挑选部分列进行导出。

      支持列换序，即列可以不按照表schema信息进行导出。

	  支持常量配置，用户需要按照JSON格式:
	  ["id", "[table]", "1", "'bazhen.csy'", "null", "COUNT(*)", "2.3" , "true"]
	  id为普通列名，[table]为包含保留在的列名，1为整形数字常量，'bazhen.csy'为字符串常量，null为空指针，to_char(a + 1)为表达式，2.3为浮点数，true为布尔值。

		column必须用户显示指定同步的列集合，不允许为空！

	* 必选：是 <br />

	* 默认值：无 <br />

* **splitPk**

	* 描述：SqlServerReader进行数据抽取时，如果指定splitPk，表示用户希望使用splitPk代表的字段进行数据分片，DataX因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能。

	  推荐splitPk用户使用表主键，因为表主键通常情况下比较均匀，因此切分出来的分片也不容易出现数据热点。

	  目前splitPk仅支持整形型数据切分，`不支持浮点、字符串、日期等其他类型`。如果用户指定其他非支持类型，SqlServerReader将报错！

		splitPk设置为空，底层将视作用户不允许对单表进行切分，因此使用单通道进行抽取。

	* 必选：否 <br />

	* 默认值：无 <br />

* **where**

	* 描述：筛选条件，MysqlReader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为gmt_create > $bizdate 。注意：不可以将where条件指定为limit 10，limit不是SQL的合法where子句。<br />

          where条件可以有效地进行业务增量同步。如果该值为空，代表同步全表所有的信息。

	* 必选：否 <br />

	* 默认值：无 <br />

* **querySql**

	* 描述：在有些业务场景下，where这一配置项不足以描述所筛选的条件，用户可以通过该配置型来自定义筛选SQL。当用户配置了这一项之后，DataX系统就会忽略table，column这些配置型，直接使用这个配置项的内容对数据进行筛选，例如需要进行多表join后同步数据，使用select a,b from table_a join table_b on table_a.id = table_b.id <br />

	 `当用户配置querySql时，SqlServerReader直接忽略table、column、where条件的配置`。

	* 必选：否 <br />

	* 默认值：无 <br />

* **fetchSize**

	* 描述：该配置项定义了插件和数据库服务器端每次批量数据获取条数，该值决定了DataX和服务器端的网络交互次数，能够较大的提升数据抽取性能。<br />

	 `注意，该值过大(>2048)可能造成DataX进程OOM。`。

	* 必选：否 <br />

	* 默认值：1024 <br />


### 3.3 类型转换

目前SqlServerReader支持大部分SqlServer类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出SqlServerReader针对SqlServer类型转换列表:


| DataX 内部类型| SqlServer 数据类型    |
| -------- | -----  |
| Long     |bigint, int, smallint, tinyint|
| Double   |float, decimal, real, numeric|
|String  |char,nchar,ntext,nvarchar,text,varchar,nvarchar(MAX),varchar(MAX)|
| Date     |date, datetime, time    |
| Boolean  |bit|
| Bytes    |binary,varbinary,varbinary(MAX),timestamp|



请注意:

* `除上述罗列字段类型外，其他类型均不支持`。
* `timestamp类型作为二进制类型`。

## 4 性能报告

暂无

## 5 约束限制

### 5.1 主备同步数据恢复问题

主备同步问题指SqlServer使用主从灾备，备库从主库不间断通过binlog恢复数据。由于主备数据同步存在一定的时间差，特别在于某些特定情况，例如网络延迟等问题，导致备库同步恢复的数据与主库有较大差别，导致从备库同步的数据不是一份当前时间的完整镜像。

针对这个问题，我们提供了preSql功能，该功能待补充。

### 5.2 一致性约束

SqlServer在数据存储划分中属于RDBMS系统，对外可以提供强一致性数据查询接口。例如当一次同步任务启动运行过程中，当该库存在其他数据写入方写入数据时，SqlServerReader完全不会获取到写入更新数据，这是由于数据库本身的快照特性决定的。关于数据库快照特性，请参看[MVCC Wikipedia](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)

上述是在SqlServerReader单线程模型下数据同步一致性的特性，由于SqlServerReader可以根据用户配置信息使用了并发数据抽取，因此不能严格保证数据一致性：当SqlServerReader根据splitPk进行数据切分后，会先后启动多个并发任务完成数据同步。由于多个并发任务相互之间不属于同一个读事务，同时多个并发任务存在时间间隔。因此这份数据并不是`完整的`、`一致的`数据快照信息。

针对多线程的一致性快照需求，在技术上目前无法实现，只能从工程角度解决，工程化的方式存在取舍，我们提供几个解决思路给用户，用户可以自行选择：

1. 使用单线程同步，即不再进行数据切片。缺点是速度比较慢，但是能够很好保证一致性。

2. 关闭其他数据写入方，保证当前数据为静态数据，例如，锁表、关闭备库同步等等。缺点是可能影响在线业务。

### 5.3 数据库编码问题

SqlServerReader底层使用JDBC进行数据抽取，JDBC天然适配各类编码，并在底层进行了编码转换。因此SqlServerReader不需用户指定编码，可以自动识别编码并转码。

### 5.4 增量数据同步

SqlServerReader使用JDBC SELECT语句完成数据抽取工作，因此可以使用SELECT...WHERE...进行增量数据抽取，方式有多种：

* 数据库在线应用写入数据库时，填充modify字段为更改时间戳，包括新增、更新、删除(逻辑删)。对于这类应用，SqlServerReader只需要WHERE条件跟上一同步阶段时间戳即可。
* 对于新增流水型数据，SqlServerReader可以WHERE条件后跟上一阶段最大自增ID即可。

对于业务上无字段区分新增、修改数据情况，SqlServerReader也无法进行增量数据同步，只能同步全量数据。

### 5.5 Sql安全性

SqlServerReader提供querySql语句交给用户自己实现SELECT抽取语句，SqlServerReader本身对querySql不做任何安全性校验。这块交由DataX用户方自己保证。

## 6 FAQ


