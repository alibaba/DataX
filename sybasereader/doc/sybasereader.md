
# SybaseReader 插件文档


___


## 1 快速介绍

SybaseReader插件实现了从Sybase读取数据。在底层实现上，SybaseReader通过JDBC连接远程Sybase数据库，并执行相应的sql语句将数据从Sybase库中SELECT出来。

## 2 实现原理

简而言之，SybaseReader通过JDBC连接器连接到远程的Sybase数据库，并根据用户配置的信息生成查询SELECT SQL语句并发送到远程Sybase数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column、Where的信息，SybaseReader将其拼接为SQL语句发送到Sybase数据库；对于用户配置querySql信息，Sybase直接将其发送到Sybase数据库。


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
                    "name": "SybaseReader",
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
     "jdbc:sybase:Tds:192.168.1.92:5000/tempdb?charset=cp936"
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
                    "name": "SybaseReader",
                    "parameter": {
                        "username": "root",
                        "password": "root",
                        "where": "",
                        "connection": [
                            {
                                "querySql": [
                                    "select db_id,on_line_flag from db_info where db_id < 10"
                                ],
                                "jdbcUrl": [
                                    "jdbc:sybase:Tds:192.168.1.92:5000/tempdb?charset=cp936"
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

    * 描述：描述的是到对端数据库的JDBC连接信息，使用JSON的数组描述，并支持一个库填写多个连接地址。之所以使用JSON数组描述连接信息，是因为阿里集团内部支持多个IP探测，如果配置了多个，SybaseReader可以依次探测ip的可连接性，直到选择一个合法的IP。如果全部连接失败，SybaseReader报错。 注意，jdbcUrl必须包含在connection配置单元中。对于阿里集团外部使用情况，JSON数组填写一个JDBC连接即可。

		jdbcUrl按照Sybase官方规范，并可以填写连接附件控制信息。具体请参看[Sybase官方文档](http://www.Sybase.com/technetwork/database/enterprise-edition/documentation/index.html)。

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

	* 描述：筛选条件，MysqlReader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为gmt_create > $bizdate 。注意：不可以将where条件指定为limit 10，limit不是SQL的合法where子句。<br />

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
	


### 3.3 类型转换

目前SybaseReader支持大部分Sybase类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出SybaseReader针对Sybase类型转换列表:


| DataX 内部类型| Sybase 数据类型    |
| -------- | -----  |
| Long     |Tinyint,Smallint,Int,Money,Smallmoney|
| Double   |Float,Real,Numeric,Decimal|
| String   |Char,Varchar,Nchar,Nvarchar,Text|
| Date     |Timestamp,Datetime,Smalldatetime|
| Boolean  |bit, bool|
| Bytes    |Binary,Varbinary,Image|



请注意:

* `除上述罗列字段类型外，其他类型均不支持`。


## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征

为了模拟线上真实数据，我们设计两个Sybase数据表，分别为:

#### 4.1.2 机器参数

* 执行DataX的机器参数为:

* Sybase数据库机器参数为:

### 4.2 测试报告

#### 4.2.1 表1测试报告


| 并发任务数| DataX速度(Rec/s)|DataX流量|网卡流量|DataX运行负载|DB运行负载|
|--------| --------|--------|--------|--------|--------|
|1| DataX 统计速度(Rec/s)|DataX统计流量|网卡流量|DataX运行负载|DB运行负载|

## 5 约束限制


### 5.1 一致性约束

Sybase在数据存储划分中属于RDBMS系统，对外可以提供强一致性数据查询接口。例如当一次同步任务启动运行过程中，当该库存在其他数据写入方写入数据时，SybaseReader完全不会获取到写入更新数据，这是由于数据库本身的快照特性决定的。关于数据库快照特性，请参看[MVCC Wikipedia](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)

上述是在SybaseReader单线程模型下数据同步一致性的特性，由于SybaseReader可以根据用户配置信息使用了并发数据抽取，因此不能严格保证数据一致性：当SybaseReader根据splitPk进行数据切分后，会先后启动多个并发任务完成数据同步。由于多个并发任务相互之间不属于同一个读事务，同时多个并发任务存在时间间隔。因此这份数据并不是`完整的`、`一致的`数据快照信息。

针对多线程的一致性快照需求，在技术上目前无法实现，只能从工程角度解决，工程化的方式存在取舍，我们提供几个解决思路给用户，用户可以自行选择：

1. 使用单线程同步，即不再进行数据切片。缺点是速度比较慢，但是能够很好保证一致性。

2. 关闭其他数据写入方，保证当前数据为静态数据，例如，锁表、关闭备库同步等等。缺点是可能影响在线业务。

### 5.2 数据库编码问题


SybaseReader底层使用JDBC进行数据抽取，JDBC天然适配各类编码，并在底层进行了编码转换。因此SybaseReader不需用户指定编码，可以自动获取编码并转码。

对于Sybase底层写入编码和其设定的编码不一致的混乱情况，SybaseReader对此无法识别，对此也无法提供解决方案，对于这类情况，`导出有可能为乱码`。

### 5.3 增量数据同步

SybaseReader使用JDBC SELECT语句完成数据抽取工作，因此可以使用SELECT...WHERE...进行增量数据抽取，方式有多种：

* 数据库在线应用写入数据库时，填充modify字段为更改时间戳，包括新增、更新、删除(逻辑删)。对于这类应用，SybaseReader只需要WHERE条件跟上一同步阶段时间戳即可。
* 对于新增流水型数据，SybaseReader可以WHERE条件后跟上一阶段最大自增ID即可。

对于业务上无字段区分新增、修改数据情况，SybaseReader也无法进行增量数据同步，只能同步全量数据。

### 5.4 Sql安全性

SybaseReader提供querySql语句交给用户自己实现SELECT抽取语句，SybaseReader本身对querySql不做任何安全性校验。这块交由DataX用户方自己保证。

## 6 FAQ

***

**Q: 目前已验证支持sybase的版本？**

  A: Sybase ASE 16/15.7

**Q: SybaseReader同步报错，报错信息为XXX**

 A: 网络或者权限问题，请使用Sybase命令行或者可视化工具进行测试：
 如果上述命令也报错，那可以证实是环境问题，请联系你的DBA。


**Q: SybaseReader抽取速度很慢怎么办？**

 A: 影响抽取时间的原因大概有如下几个：
  1.  由于SQL的plan异常，导致的抽取时间长； 在抽取时，尽可能使用全表扫描代替索引扫描;
  2.  合理sql的并发度，减少抽取时间；根据表的大小，
  3.  设置合理fetchsize，减少网络IO;
