# DataX AdbMysqlWriter


---


## 1 快速介绍

AdbMysqlWriter 插件实现了写入数据到 ADB MySQL 目的表的功能。在底层实现上， AdbMysqlWriter 通过 JDBC 连接远程 ADB MySQL 数据库，并执行相应的 `insert into ...` 或者 ( `replace into ...` ) 的 SQL 语句将数据写入 ADB MySQL，内部会分批次提交入库。

AdbMysqlWriter 面向ETL开发工程师，他们使用 AdbMysqlWriter 从数仓导入数据到 ADB MySQL。同时 AdbMysqlWriter 亦可以作为数据迁移工具为DBA等用户提供服务。


## 2 实现原理

AdbMysqlWriter 通过 DataX 框架获取 Reader 生成的协议数据，AdbMysqlWriter 通过 JDBC 连接远程 ADB MySQL 数据库，并执行相应的 `insert into ...` 或者 ( `replace into ...` ) 的 SQL 语句将数据写入 ADB MySQL。


* `insert into...`(遇到主键重复时会自动忽略当前写入数据，不做更新，作用等同于`insert ignore into`)

##### 或者

* `replace into...`(没有遇到主键/唯一性索引冲突时，与 insert into 行为一致，冲突时会用新行替换原有行所有字段) 的语句写入数据到 MySQL。出于性能考虑，采用了 `PreparedStatement + Batch`，并且设置了：`rewriteBatchedStatements=true`，将数据缓冲到线程上下文 Buffer 中，当 Buffer 累计到预定阈值时，才发起写入请求。

<br />

    注意：整个任务至少需要具备 `insert/replace into...` 的权限，是否需要其他权限，取决于你任务配置中在 preSql 和 postSql 中指定的语句。


## 3 功能说明

### 3.1 配置样例

* 这里使用一份从内存产生到 ADB MySQL 导入的数据。

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
                    "name": "adbmysqlwriter",
                    "parameter": {
                        "writeMode": "replace",
                        "username": "root",
                        "password": "root",
                        "column": [
                            "*"
                        ],
                        "preSql": [
                            "truncate table @table"
                        ],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://ip:port/database?useUnicode=true",
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

    * 描述：目的数据库的 JDBC 连接信息。作业运行时，DataX 会在你提供的 jdbcUrl 后面追加如下属性：yearIsDateType=false&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true

               注意：1、在一个数据库上只能配置一个 jdbcUrl
                    2、一个 AdbMySQL 写入任务仅能配置一个 jdbcUrl
                    3、jdbcUrl按照MySQL官方规范，并可以填写连接附加控制信息，比如想指定连接编码为 gbk ，则在 jdbcUrl 后面追加属性 useUnicode=true&characterEncoding=gbk。具体请参看 Mysql官方文档或者咨询对应 DBA。

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

	* 描述：目的表的表名称。只能配置一个 AdbMySQL 的表名称。

               注意：table 和 jdbcUrl 必须包含在 connection 配置单元中

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id", "name", "age"]。如果要依次写入全部列，使用`*`表示, 例如: `"column": ["*"]`。

			**column配置项必须指定，不能留空！**

               注意：1、我们强烈不推荐你这样配置，因为当你目的表字段个数、类型等有改动时，你的任务可能运行不正确或者失败
                    2、 column 不能配置任何常量值

	* 必选：是 <br />

	* 默认值：否 <br />

* **session**

	* 描述: DataX在获取 ADB MySQL 连接时，执行session指定的SQL语句，修改当前connection session属性

	* 必须: 否

	* 默认值: 空

* **preSql**

	* 描述：写入数据到目的表前，会先执行这里的标准语句。如果 Sql 中有你需要操作到的表名称，请使用 `@table` 表示，这样在实际执行 SQL 语句时，会对变量按照实际表名称进行替换。比如希望导入数据前，先对表中数据进行删除操作，那么你可以这样配置：`"preSql":["truncate table @table"]`，效果是：在执行到每个表写入数据前，会先执行对应的 `truncate table 对应表名称` <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **postSql**

	* 描述：写入数据到目的表后，会执行这里的标准语句。（原理同 preSql ） <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **writeMode**

	* 描述：控制写入数据到目标表采用 `insert into` 或者 `replace into` 或者 `ON DUPLICATE KEY UPDATE` 语句<br />

	* 必选：是 <br />
	
	* 所有选项：insert/replace/update <br />

	* 默认值：replace <br />

* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与 Adb MySQL 的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。<br />

	* 必选：否 <br />

	* 默认值：2048 <br />


### 3.3 类型转换

目前 AdbMysqlWriter 支持大部分 MySQL 类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出 AdbMysqlWriter 针对 MySQL 类型转换列表:

| DataX 内部类型 | AdbMysql 数据类型                 |
|---------------|---------------------------------|
| Long          | tinyint, smallint, int, bigint  |
| Double        | float, double, decimal          |
| String        | varchar                         |
| Date          | date, time, datetime, timestamp |
| Boolean       | boolean                         |
| Bytes         | binary                          |

## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征
TPC-H 数据集 lineitem 表，共 17 个字段, 随机生成总记录行数 59986052。未压缩总数据量：7.3GiB

建表语句：

	CREATE TABLE `datax_adbmysqlwriter_perf_lineitem` (
		`l_orderkey` bigint NOT NULL COMMENT '',
		`l_partkey` int NOT NULL COMMENT '',
		`l_suppkey` int NOT NULL COMMENT '',
		`l_linenumber` int NOT NULL COMMENT '',
		`l_quantity` decimal(15,2) NOT NULL COMMENT '',
		`l_extendedprice` decimal(15,2) NOT NULL COMMENT '',
		`l_discount` decimal(15,2) NOT NULL COMMENT '',
		`l_tax` decimal(15,2) NOT NULL COMMENT '',
		`l_returnflag` varchar(1024) NOT NULL COMMENT '',
		`l_linestatus` varchar(1024) NOT NULL COMMENT '',
		`l_shipdate` date NOT NULL COMMENT '',
		`l_commitdate` date NOT NULL COMMENT '',
		`l_receiptdate` date NOT NULL COMMENT '',
		`l_shipinstruct` varchar(1024) NOT NULL COMMENT '',
		`l_shipmode` varchar(1024) NOT NULL COMMENT '',
		`l_comment` varchar(1024) NOT NULL COMMENT '',
		`dummy` varchar(1024),
		PRIMARY KEY (`l_orderkey`, `l_linenumber`)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='datax perf test';

单行记录类似于：

   	        l_orderkey: 2122789
             l_partkey: 1233571
             l_suppkey: 8608
		  l_linenumber: 1
		    l_quantity: 35.00
       l_extendedprice: 52657.85
		    l_discount: 0.02
		         l_tax: 0.07
          l_returnflag: N
		  l_linestatus: O
		    l_shipdate: 1996-11-03
		  l_commitdate: 1996-12-07
		 l_receiptdate: 1996-11-16
		l_shipinstruct: COLLECT COD
		    l_shipmode: FOB
		     l_comment: ld, regular theodolites.
                 dummy:

#### 4.1.2 机器参数

* DataX ECS: 24Core48GB

* Adb MySQL 数据库
	* 计算资源：16Core64GB（集群版）
	* 弹性IO资源：3

#### 4.1.3 DataX jvm 参数

	-Xms1G -Xmx10G -XX:+HeapDumpOnOutOfMemoryError

### 4.2 测试报告

| 通道数 | 批量提交行数 | DataX速度(Rec/s)   | DataX流量(MB/s) | 导入用时(s) |
|-----|-------|------------------|---------------|---------|
| 1   | 512   | 23071            | 2.34          | 2627    |
| 1   | 1024  | 26080            | 2.65          | 2346    |
| 1   | 2048  | 28162            | 2.86          | 2153    |
| 1   | 4096  | 28978            | 2.94          | 2119    |
| 4   | 512   | 56590            | 5.74          | 1105    |
| 4   | 1024  | 81062            | 8.22          | 763     |
| 4   | 2048  | 107117           | 10.87         | 605     |
| 4   | 4096  | 113181           | 11.48         | 579     |
| 8   | 512   | 81062            | 8.22          | 786     |
| 8   | 1024  | 127629           | 12.95         | 519     |
| 8   | 2048  | 187456           | 19.01         | 369     |
| 8   | 4096  | 206848           | 20.98         | 341     |
| 16  | 512   | 130404           | 13.23         | 513     |
| 16  | 1024  | 214235           | 21.73         | 335     |
| 16  | 2048  | 299930           | 30.42         | 253     |
| 16  | 4096  | 333255           | 33.80         | 227     |
| 32  | 512   | 206848           | 20.98         | 347     |
| 32  | 1024  | 315716           | 32.02         | 241     |
| 32  | 2048  | 399907           | 40.56         | 199     |
| 32  | 4096  | 461431           | 46.80         | 184     |
| 64  | 512   | 333255           | 33.80         | 231     |
| 64  | 1024  | 399907           | 40.56         | 204     |
| 64  | 2048  | 428471           | 43.46         | 199     |
| 64  | 4096  | 461431           | 46.80         | 187     |
| 128 | 512   | 333255           | 33.80         | 235     |
| 128 | 1024  | 399907           | 40.56         | 203     |
| 128 | 2048  | 425432           | 43.15         | 197     |
| 128 | 4096  | 387006           | 39.26         | 211     |

说明：

1. datax 使用 txtfilereader 读取本地文件，避免源端存在性能瓶颈。

#### 性能测试小结
1. channel通道个数和batchSize对性能影响比较大
2. 通常不建议写入数据库时，通道个数 > 32

## 5 约束限制

## FAQ

***

**Q: AdbMysqlWriter 执行 postSql 语句报错，那么数据导入到目标数据库了吗?**

A: DataX 导入过程存在三块逻辑，pre 操作、导入操作、post 操作，其中任意一环报错，DataX 作业报错。由于 DataX 不能保证在同一个事务完成上述几个操作，因此有可能数据已经落入到目标端。

***

**Q: 按照上述说法，那么有部分脏数据导入数据库，如果影响到线上数据库怎么办?**

A: 目前有两种解法，第一种配置 pre 语句，该 sql 可以清理当天导入数据， DataX 每次导入时候可以把上次清理干净并导入完整数据。第二种，向临时表导入数据，完成后再 rename 到线上表。

***

**Q: 上面第二种方法可以避免对线上数据造成影响，那我具体怎样操作?**

A: 可以配置临时表导入
