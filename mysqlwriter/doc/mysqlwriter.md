# DataX MysqlWriter


---


## 1 快速介绍

MysqlWriter 插件实现了写入数据到 Mysql 主库的目的表的功能。在底层实现上， MysqlWriter 通过 JDBC 连接远程 Mysql 数据库，并执行相应的 insert into ... 或者 ( replace into ...) 的 sql 语句将数据写入 Mysql，内部会分批次提交入库，需要数据库本身采用 InnoDB 引擎。

MysqlWriter 面向ETL开发工程师，他们使用 MysqlWriter 从数仓导入数据到 Mysql。同时 MysqlWriter 亦可以作为数据迁移工具为DBA等用户提供服务。


## 2 实现原理

MysqlWriter 通过 DataX 框架获取 Reader 生成的协议数据，根据你配置的 `writeMode` 生成


* `insert into...`(当主键/唯一性索引冲突时会写不进去冲突的行)

##### 或者

* `replace into...`(没有遇到主键/唯一性索引冲突时，与 insert into 行为一致，冲突时会用新行替换原有行所有字段) 的语句写入数据到 Mysql。出于性能考虑，采用了 `PreparedStatement + Batch`，并且设置了：`rewriteBatchedStatements=true`，将数据缓冲到线程上下文 Buffer 中，当 Buffer 累计到预定阈值时，才发起写入请求。

<br />

    注意：目的表所在数据库必须是主库才能写入数据；整个任务至少需要具备 insert/replace into...的权限，是否需要其他权限，取决于你任务配置中在 preSql 和 postSql 中指定的语句。


## 3 功能说明

### 3.1 配置样例

* 这里使用一份从内存产生到 Mysql 导入的数据。

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
                    "name": "mysqlwriter",
                    "parameter": {
                        "writeMode": "insert",
                        "username": "root",
                        "password": "root",
                        "column": [
                            "id",
                            "name"
                        ],
                        "session": [
                        	"set session sql_mode='ANSI'"
                        ],
                        "preSql": [
                            "delete from test"
                        ],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://127.0.0.1:3306/datax?useUnicode=true&characterEncoding=gbk",
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

               注意：1、在一个数据库上只能配置一个 jdbcUrl 值。这与 MysqlReader 支持多个备库探测不同，因为此处不支持同一个数据库存在多个主库的情况(双主导入数据情况)
                    2、jdbcUrl按照Mysql官方规范，并可以填写连接附加控制信息，比如想指定连接编码为 gbk ，则在 jdbcUrl 后面追加属性 useUnicode=true&characterEncoding=gbk。具体请参看 Mysql官方文档或者咨询对应 DBA。


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

	* 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。如果要依次写入全部列，使用`*`表示, 例如: `"column": ["*"]`。

			**column配置项必须指定，不能留空！**

               注意：1、我们强烈不推荐你这样配置，因为当你目的表字段个数、类型等有改动时，你的任务可能运行不正确或者失败
                    2、 column 不能配置任何常量值

	* 必选：是 <br />

	* 默认值：否 <br />

* **session**

	* 描述: DataX在获取Mysql连接时，执行session指定的SQL语句，修改当前connection session属性

	* 必须: 否

	* 默认值: 空

* **preSql**

	* 描述：写入数据到目的表前，会先执行这里的标准语句。如果 Sql 中有你需要操作到的表名称，请使用 `@table` 表示，这样在实际执行 Sql 语句时，会对变量按照实际表名称进行替换。比如你的任务是要写入到目的端的100个同构分表(表名称为:datax_00,datax01, ... datax_98,datax_99)，并且你希望导入数据前，先对表中数据进行删除操作，那么你可以这样配置：`"preSql":["delete from 表名"]`，效果是：在执行到每个表写入数据前，会先执行对应的 delete from 对应表名称 <br />

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

	* 默认值：insert <br />

* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与Mysql的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。<br />

	* 必选：否 <br />

	* 默认值：1024 <br />


### 3.3 类型转换

类似 MysqlReader ，目前 MysqlWriter 支持大部分 Mysql 类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出 MysqlWriter 针对 Mysql 类型转换列表:


| DataX 内部类型| Mysql 数据类型    |
| -------- | -----  |
| Long     |int, tinyint, smallint, mediumint, int, bigint, year|
| Double   |float, double, decimal|
| String   |varchar, char, tinytext, text, mediumtext, longtext    |
| Date     |date, datetime, timestamp, time    |
| Boolean  |bit, bool   |
| Bytes    |tinyblob, mediumblob, blob, longblob, varbinary    |

 * `bit类型目前是未定义类型转换`

## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征
建表语句：

	CREATE TABLE `datax_mysqlwriter_perf_00` (
  	`biz_order_id` bigint(20) NOT NULL AUTO_INCREMENT  COMMENT 'id',
  	`key_value` varchar(4000) NOT NULL COMMENT 'Key-value的内容',
  	`gmt_create` datetime NOT NULL COMMENT '创建时间',
  	`gmt_modified` datetime NOT NULL COMMENT '修改时间',
  	`attribute_cc` int(11) DEFAULT NULL COMMENT '防止并发修改的标志',
  	`value_type` int(11) NOT NULL DEFAULT '0' COMMENT '类型',
  	`buyer_id` bigint(20) DEFAULT NULL COMMENT 'buyerid',
  	`seller_id` bigint(20) DEFAULT NULL COMMENT 'seller_id',
  	PRIMARY KEY (`biz_order_id`,`value_type`),
  	KEY `idx_biz_vertical_gmtmodified` (`gmt_modified`)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='datax perf test'


单行记录类似于：

   	   key_value: ;orderIds:20148888888,2014888888813800;
  	  gmt_create: 2011-09-24 11:07:20
	gmt_modified: 2011-10-24 17:56:34
	attribute_cc: 1
  	  value_type: 3
    	buyer_id: 8888888
   	   seller_id: 1

#### 4.1.2 机器参数

* 执行DataX的机器参数为:
	1. cpu: 24核 Intel(R) Xeon(R) CPU E5-2630 0 @ 2.30GHz
	2. mem: 48GB
	3. net: 千兆双网卡
	4. disc: DataX 数据不落磁盘，不统计此项

* Mysql数据库机器参数为:
	1. cpu: 32核 Intel(R) Xeon(R) CPU E5-2650 v2 @ 2.60GHz
	2. mem: 256GB
	3. net: 千兆双网卡
	4. disc: BTWL419303E2800RGN  INTEL SSDSC2BB800G4   D2010370

#### 4.1.3 DataX jvm 参数

	-Xms1024m -Xmx1024m -XX:+HeapDumpOnOutOfMemoryError


### 4.2 测试报告

#### 4.2.1 单表测试报告


| 通道数|  批量提交行数| DataX速度(Rec/s)|DataX流量(MB/s)| DataX机器网卡流出流量(MB/s)|DataX机器运行负载|DB网卡进入流量(MB/s)|DB运行负载|DB TPS|
|--------|--------| --------|--------|--------|--------|--------|--------|--------|
|1| 128 | 5319 | 0.260 | 0.580 | 0.05 | 0.620| 0.5 | 50 |
|1| 512 | 14285 | 0.697 | 1.6 | 0.12 | 1.6 | 0.6 | 28 |
|1| 1024 | 17241 | 0.842  | 1.9 | 0.20 | 1.9 | 0.6 | 16|
|1| 2048 | 31250 | 1.49 | 2.8 | 0.15 | 3.0| 0.8 | 15 |
|1| 4096 | 31250 | 1.49 | 3.5 | 0.20 | 3.6| 0.8 | 8 |
|4| 128 | 11764 | 0.574 | 1.5 | 0.21 | 1.6| 0.8 | 112 |
|4| 512 | 30769 | 1.47 | 3.5 | 0.3 | 3.6 | 0.9 | 88 |
|4| 1024 | 50000 | 2.38 | 5.4 | 0.3 | 5.5 | 1.0 | 66 |
|4| 2048 | 66666 | 3.18 | 7.0 | 0.3 | 7.1| 1.37 | 46 |
|4| 4096 | 80000 | 3.81 | 7.3| 0.5 | 7.3| 1.40 | 26 |
|8| 128 | 17777 | 0.868 | 2.9 | 0.28 | 2.9| 0.8 | 200 |
|8| 512 | 57142 | 2.72 | 8.5 | 0.5 | 8.5| 0.70 | 159 |
|8| 1024 | 88888 | 4.24 | 12.2 | 0.9 | 12.4 | 1.0 | 108 |
|8| 2048 | 133333 | 6.36 | 14.7 | 0.9 | 14.7 | 1.0 | 81 |
|8| 4096 | 166666 | 7.95 | 19.5 | 0.9 | 19.5 | 3.0 | 45 |
|16| 128 | 32000 | 1.53 | 3.3 | 0.6 | 3.4 | 0.88 | 401 |
|16| 512 | 106666 | 5.09 | 16.1| 0.9 | 16.2 | 2.16 | 260 |
|16| 1024 | 173913 | 8.29 | 22.1| 1.5 | 22.2 | 4.5 | 200 |
|16| 2048 | 228571 | 10.90 | 28.6 | 1.61 | 28.7 | 4.60 | 128 |
|16| 4096 | 246153 | 11.74 | 31.1| 1.65 | 31.2| 4.66 | 57 |
|32| 1024 | 246153 | 11.74 | 30.5| 3.17 | 30.7 | 12.10 | 270 |


说明：

1. 这里的单表，主键类型为 bigint(20),自增。
2. batchSize 和 通道个数，对性能影响较大。
3. 16通道，4096批量提交时，出现 full gc 2次。


#### 4.2.2 分表测试报告(2个分库，每个分库4张分表，共计8张分表)


| 通道数|  批量提交行数| DataX速度(Rec/s)|DataX流量(MB/s)| DataX机器网卡流出流量(MB/s)|DataX机器运行负载|DB网卡进入流量(MB/s)|DB运行负载|DB TPS|
|--------|--------| --------|--------|--------|--------|--------|--------|--------|
|8| 128 | 26764 | 1.28 | 2.9 | 0.5 | 3.0| 0.8 | 209 |
|8| 512 | 95180 | 4.54 | 10.5 | 0.7 | 10.9 | 0.8 | 188 |
|8| 1024 | 94117 | 4.49  | 12.3 | 0.6 | 12.4 | 1.09 | 120 |
|8| 2048 | 133333 | 6.36 | 19.4 | 0.9 | 19.5| 1.35 | 85 |
|8| 4096 | 191692 | 9.14 | 22.1 | 1.0 | 22.2| 1.45 | 45 |


#### 4.2.3 分表测试报告(2个分库，每个分库8张分表，共计16张分表)


| 通道数|  批量提交行数| DataX速度(Rec/s)|DataX流量(MB/s)| DataX机器网卡流出流量(MB/s)|DataX机器运行负载|DB网卡进入流量(MB/s)|DB运行负载|DB TPS|
|--------|--------| --------|--------|--------|--------|--------|--------|--------|
|16| 128 | 50124 | 2.39 | 5.6 | 0.40 | 6.0| 2.42 | 378 |
|16| 512 | 155084 | 7.40 | 18.6 | 1.30 | 18.9| 2.82 | 325 |
|16| 1024 | 177777 | 8.48 | 24.1 | 1.43 | 25.5| 3.5 | 233 |
|16| 2048 | 289382 | 13.8 | 33.1 | 2.5 | 33.5| 4.5 | 150 |
|16| 4096 | 326451 | 15.52 | 33.7 | 1.5 | 33.9| 4.3 | 80 |

#### 4.2.4 性能测试小结
1. 批量提交行数（batchSize）对性能影响很大，当 `batchSize>=512` 之后，单线程写入速度能达到每秒写入一万行
2. 在 `batchSize>=512` 的基础上，随着通道数的增加（通道数<32），速度呈线性比增加。
3. `通常不建议写入数据库时，通道个数 >32`


## 5 约束限制




## FAQ

***

**Q: MysqlWriter 执行 postSql 语句报错，那么数据导入到目标数据库了吗?**

A: DataX 导入过程存在三块逻辑，pre 操作、导入操作、post 操作，其中任意一环报错，DataX 作业报错。由于 DataX 不能保证在同一个事务完成上述几个操作，因此有可能数据已经落入到目标端。

***

**Q: 按照上述说法，那么有部分脏数据导入数据库，如果影响到线上数据库怎么办?**

A: 目前有两种解法，第一种配置 pre 语句，该 sql 可以清理当天导入数据， DataX 每次导入时候可以把上次清理干净并导入完整数据。第二种，向临时表导入数据，完成后再 rename 到线上表。

***

**Q: 上面第二种方法可以避免对线上数据造成影响，那我具体怎样操作?**

A: 可以配置临时表导入
