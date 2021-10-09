
# DrdsReader 插件文档


___


## 1 快速介绍

DrdsReader插件实现了从DRDS(分布式RDS)读取数据。在底层实现上，DrdsReader通过JDBC连接远程DRDS数据库，并执行相应的sql语句将数据从DRDS库中SELECT出来。

DRDS的插件目前DataX只适配了Mysql引擎的场景，DRDS对于DataX而言，就是一套分布式Mysql数据库，并且大部分通信协议遵守Mysql使用场景。

## 2 实现原理

简而言之，DrdsReader通过JDBC连接器连接到远程的DRDS数据库，并根据用户配置的信息生成查询SELECT SQL语句并发送到远程DRDS数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column、Where的信息，DrdsReader将其拼接为SQL语句发送到DRDS数据库。不同于普通的Mysql数据库，DRDS作为分布式数据库系统，无法适配所有Mysql的协议，包括复杂的Join等语句，DRDS暂时无法支持。


## 3 功能说明

### 3.1 配置样例

* 配置一个从DRDS数据库同步抽取数据到本地的作业:

```
{
    "job": {
        "setting": {
            "speed": {
            //设置传输速度，单位为byte/s，DataX运行会尽可能达到该速度但是不超过它.
                 "byte": 1048576
            }
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
                    "name": "drdsReader",
                    "parameter": {
                        // 数据库连接用户名
                        "username": "root",
                        // 数据库连接密码
                        "password": "root",
                        "column": [
                            "id","name"
                        ],
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
        },
        "content": [
            {
                "reader": {
                    "name": "drdsreader",
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
                                    "jdbc:drds://localhost:3306/database"]
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

	* 描述：描述的是到对端数据库的JDBC连接信息，使用JSON的数组描述.注意，jdbcUrl必须包含在connection配置单元中。DRDSReader中关于jdbcUrl中JSON数组填写一个JDBC连接即可。
	
		jdbcUrl按照Mysql官方规范，并可以填写连接附件控制信息。具体请参看[mysql官方文档](http://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html)。
 
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

	* 描述：所选取需要抽取的表。注意，由于DRDS本身就是分布式数据源，因此填写多张表无意义。系统对多表不做校验。<br />
 
	* 必选：是 <br />
 
	* 默认值：无 <br />
 
* **column**

	* 描述：所配置的表中需要同步的列名集合，使用JSON的数组描述字段信息。用户使用\*代表默认使用所有列配置，例如['\*']。	
	
	  支持列裁剪，即列可以挑选部分列进行导出。

      支持列换序，即列可以不按照表schema信息进行导出。
      
	  支持常量配置，用户需要按照Mysql SQL语法格式:
	  ["id", "\`table\`", "1", "'bazhen.csy'", "null", "to_char(a + 1)", "2.3" , "true"]
	  id为普通列名，\`table\`为包含保留在的列名，1为整形数字常量，'bazhen.csy'为字符串常量，null为空指针，to_char(a + 1)为表达式，2.3为浮点数，true为布尔值。

		column必须用户显示指定同步的列集合，不允许为空！

	* 必选：是 <br />
 
	* 默认值：无 <br />

* **where**
 
	* 描述：筛选条件，DrdsReader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为gmt_create > $bizdate 。<br />。
	
          where条件可以有效地进行业务增量同步。where条件不配置或者为空，视作全表同步数据。
 
	* 必选：否 <br />
 
	* 默认值：无 <br />

* **querySql**

	* 描述：暂时不支持配置querySql模式 <br />


### 3.3 类型转换

目前DrdsReader支持大部分DRDS类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出DrdsReader针对DRDS类型转换列表:


| DataX 内部类型| DRDS 数据类型    |
| -------- | -----  |
| Long     |int, tinyint, smallint, mediumint, int, bigint|
| Double   |float, double, decimal|
| String   |varchar, char, tinytext, text, mediumtext, longtext    | 
| Date     |date, datetime, timestamp, time, year    | 
| Boolean  |bit, bool   |  
| Bytes    |tinyblob, mediumblob, blob, longblob, varbinary    | 


请注意:

* `除上述罗列字段类型外，其他类型均不支持`。
* `类似Mysql，tinyint(1)视作整形`。
* `类似Mysql，bit类型读取目前是未定义状态。`

## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征
建表语句：

	CREATE TABLE `tc_biz_vertical_test_0000` (
  	`biz_order_id` bigint(20) NOT NULL COMMENT 'id',
  	`key_value` varchar(4000) NOT NULL COMMENT 'Key-value的内容',
  	`gmt_create` datetime NOT NULL COMMENT '创建时间',
  	`gmt_modified` datetime NOT NULL COMMENT '修改时间',
  	`attribute_cc` int(11) DEFAULT NULL COMMENT '防止并发修改的标志',
  	`value_type` int(11) NOT NULL DEFAULT '0' COMMENT '类型',
  	`buyer_id` bigint(20) DEFAULT NULL COMMENT 'buyerid',
  	`seller_id` bigint(20) DEFAULT NULL COMMENT 'seller_id',
  	PRIMARY KEY (`biz_order_id`,`value_type`),
  	KEY `idx_biz_vertical_gmtmodified` (`gmt_modified`)
	) ENGINE=InnoDB DEFAULT CHARSET=gbk COMMENT='tc_biz_vertical'


单行记录类似于：

	biz_order_id: 888888888
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

* DRDS数据库机器参数为:
	1. cpu: 32核 Intel(R) Xeon(R) CPU E5-2650 v2 @ 2.60GHz
	2. mem: 256GB
	3. net: 千兆双网卡
	4. disc: BTWL419303E2800RGN  INTEL SSDSC2BB800G4   D2010370

#### 4.1.3 DataX jvm 参数

	-Xms1024m -Xmx1024m -XX:+HeapDumpOnOutOfMemoryError


### 4.2 测试报告

#### 4.2.1 单表测试报告


| 通道数| 是否按照主键切分| DataX速度(Rec/s)| DataX机器运行负载|DB网卡流出流量(MB/s)|DB运行负载|
|--------|--------| --------|--------|--------|--------|--------|--------|


说明：

1. 这里的单表，主键类型为 bigint(20),范围为：190247559466810-570722244711460，从主键范围划分看，数据分布均匀。
2. 对单表如果没有安装主键切分，那么配置通道个数不会提升速度，效果与1个通道一样。


#### 4.2.2 分表测试报告(2个分库，每个分库16张分表，共计32张分表)


| 通道数| DataX速度(Rec/s)|DataX机器运行负载|DB网卡流出流量(MB/s)|DB运行负载|
|--------| --------|--------|--------|--------|--------|--------|



## 5 约束限制

	
### 5.1 一致性视图问题

DRDS本身属于分布式数据库，对外无法提供一致性的多库多表视图，不同于Mysql等单库单表同步，DRDSReader无法抽取同一个时间切片的分库分表快照信息，也就是说DataX DrdsReader抽取底层不同的分表将获取不同的分表快照，无法保证强一致性。

	
### 5.2 数据库编码问题

DRDS本身的编码设置非常灵活，包括指定编码到库、表、字段级别，甚至可以均不同编码。优先级从高到低为字段、表、库、实例。我们不推荐数据库用户设置如此混乱的编码，最好在库级别就统一到UTF-8。

DrdsReader底层使用JDBC进行数据抽取，JDBC天然适配各类编码，并在底层进行了编码转换。因此DrdsReader不需用户指定编码，可以自动获取编码并转码。
	
对于DRDS底层写入编码和其设定的编码不一致的混乱情况，DrdsReader对此无法识别，对此也无法提供解决方案，对于这类情况，`导出有可能为乱码`。
	
### 5.3 增量数据同步

DrdsReader使用JDBC SELECT语句完成数据抽取工作，因此可以使用SELECT...WHERE...进行增量数据抽取，方式有多种：

* 数据库在线应用写入数据库时，填充modify字段为更改时间戳，包括新增、更新、删除(逻辑删)。对于这类应用，DrdsReader只需要WHERE条件跟上一同步阶段时间戳即可。
* 对于新增流水型数据，DrdsReader可以WHERE条件后跟上一阶段最大自增ID即可。
	
对于业务上无字段区分新增、修改数据情况，DrdsReader也无法进行增量数据同步，只能同步全量数据。
	
### 5.4 Sql安全性

DrdsReader提供querySql语句交给用户自己实现SELECT抽取语句，DrdsReader本身对querySql不做任何安全性校验。这块交由DataX用户方自己保证。
	
## 6 FAQ

***

**Q: DrdsReader同步报错，报错信息为XXX**
 
 A: 网络或者权限问题，请使用DRDS命令行测试：
 
   mysql -u<username> -p<password> -h<ip> -D<database> -e "select * from <表名>"

如果上述命令也报错，那可以证实是环境问题，请联系你的DBA。

***

**Q: 我想同步DRDS增量数据，怎么配置?**
 
 A: DrdsReader必须业务支持增量字段DataX才能同步增量，例如在淘宝大部分业务表中，通过gmt_modified字段表征这条记录的最新修改时间，那么DataX DrdsReader只需要配置where条件为
 
```
    "where": "Date(add_time) = '2014-06-01'"
```

*** 



