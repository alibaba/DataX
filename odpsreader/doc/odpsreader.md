# DataX ODPSReader


---


## 1 快速介绍
ODPSReader 实现了从 ODPS读取数据的功能，有关ODPS请参看(https://help.aliyun.com/document_detail/27800.html?spm=5176.doc27803.6.101.NxCIgY)。 在底层实现上，ODPSReader 根据你配置的 源头项目 / 表 / 分区 / 表字段 等信息，通过 `Tunnel` 从 ODPS 系统中读取数据。

<br />

     注意  1、如果你需要使用ODPSReader/Writer插件，由于 AccessId/AccessKey 解密的需要，请务必使用 JDK 1.6.32 及以上版本。JDK 安装事项，请联系 PE 处理
          2、ODPSReader 不是通过 ODPS SQL （select ... from ... where ... ）来抽取数据的
          3、注意区分你要读取的表是线上环境还是线下环境
          4、目前 DataX3 依赖的 SDK 版本是：
                    <dependency>
                        <groupId>com.aliyun.odps</groupId>
                        <artifactId>odps-sdk-core-internal</artifactId>
                        <version>0.13.2</version>
                    </dependency>


## 2 实现原理
ODPSReader 支持读取分区表、非分区表，不支持读取虚拟视图。当要读取分区表时，需要指定出具体的分区配置，比如读取 t0 表，其分区为 pt=1,ds=hangzhou  那么你需要在配置中配置该值。当要读取非分区表时，你不能提供分区配置。表字段可以依序指定全部列，也可以指定部分列，或者调整列顺序，或者指定常量字段，但是表字段中不能指定分区列（分区列不是表字段）。

    注意：要特别注意 odpsServer、project、table、accessId、accessKey 的配置，因为直接影响到是否能够加载到你需要读取数据的表。很多权限问题都出现在这里。


## 3 功能说明

### 3.1 配置样例

* 这里使用一份读出 ODPS 数据然后打印到屏幕的配置样板。

```
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
                    "name": "odpsreader",
                    "parameter": {
                        "accessId": "accessId",
                        "accessKey": "accessKey",
                        "project": "targetProjectName",
                        "table": "tableName",
                        "partition": [
                            "pt=1,ds=hangzhou"
                        ],
                        "column": [
                            "customer_id",
                            "nickname"
                        ],
                        "packageAuthorizedProject": "yourCurrentProjectName",
                        "splitMode": "record",
                        "odpsServer": "http://xxx/api",
                        "tunnelServer": "http://dt.odps.aliyun.com"
                    }
                },
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "fieldDelimiter": "\t",
                        "print": "true"
                    }
                }
            }
        ]
    }
}

```


### 3.2 参数说明

## 参数

* **accessId**
	* 描述：ODPS系统登录ID <br />

 	* 必选：是 <br />

 	* 默认值：无 <br />

* **accessKey**
	* 描述：ODPS系统登录Key <br />

 	* 必选：是 <br />

 	* 默认值：无 <br />

* **project**

	* 描述：读取数据表所在的 ODPS 项目名称（大小写不敏感） <br />

	* 必选：是 <br />

 	* 默认值：无 <br />

* **table**

 	* 描述：读取数据表的表名称（大小写不敏感） <br />

 	* 必选：是 <br />

 	* 默认值：无 <br />

* **partition**

	* 描述：读取数据所在的分区信息，支持linux shell通配符，包括 * 表示0个或多个字符，?代表任意一个字符。例如现在有分区表 test，其存在 pt=1,ds=hangzhou   pt=1,ds=shanghai   pt=2,ds=hangzhou   pt=2,ds=beijing 四个分区，如果你想读取 pt=1,ds=shanghai 这个分区的数据，那么你应该配置为: `"partition":["pt=1,ds=shanghai"]`； 如果你想读取 pt=1下的所有分区，那么你应该配置为: `"partition":["pt=1,ds=* "]`；如果你想读取整个 test 表的所有分区的数据，那么你应该配置为: `"partition":["pt=*,ds=*"]` <br />

	* 必选：如果表为分区表，则必填。如果表为非分区表，则不能填写 <br />

	* 默认值：无 <br />

* **column**

	* 描述：读取 odps 源头表的列信息。例如现在有表 test，其字段为：id,name,age  如果你想依次读取 id,name,age  那么你应该配置为: `"column":["id","name","age"]` 或者配置为:`"column"=["*"]`  这里 * 表示依次读取表的每个字段，但是我们不推荐你配置抽取字段为 * ，因为当你的表字段顺序调整、类型变更或者个数增减，你的任务就会存在源头表列和目的表列不能对齐的风险，会直接导致你的任务运行结果不正确甚至运行失败。如果你想依次读取 name,id  那么你应该配置为: `"coulumn":["name","id"]`  如果你想在源头抽取的字段中添加常量字段(以适配目标表的字段顺序)，比如你想抽取的每一行数据值为 age 列对应的值,name列对应的值,常量日期值1988-08-08 08:08:08,id 列对应的值   那么你应该配置为:`"column":["age","name","'1988-08-08 08:08:08'","id"]`    即常量列首尾用符号`'` 包住即可，我们内部实现上识别常量是通过检查你配置的每一个字段，如果发现有字段首尾都有`'`，则认为其是常量字段，其实际值为去除`'` 之后的值。

               注意：ODPSReader 抽取数据表不是通过 ODPS 的 Select SQL 语句，所以不能在字段上指定函数，也不能指定分区字段名称（分区字段不属于表字段）

	* 必选：是 <br />

	* 默认值：无 <br />

* **odpsServer**

	* 描述：源头表 所在 ODPS 系统的server 地址 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **tunnelServer**

	* 描述：源头表 所在 ODPS 系统的tunnel 地址 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **splitMode**

	* 描述：读取源头表时切分所需要的模式。默认值为 record，可不填，表示根据切分份数，按照记录数进行切分。如果你的任务目的端为 Mysql，并且是 Mysql 的多个表，那么根据现在 DataX 结构，你的源头表必须是分区表，并且每个分区依次对应目的端 Mysql 的多个分表，则此时应该配置为`"splitMode":"partition"`  <br />

	* 必选：否 <br />

	* 默认值：record <br />

* **accountProvider** [待定]

	* 描述：读取时使用的 ODPS 账号类型。目前支持 aliyun/taobao 两种类型。默认为 aliyun，可不填 <br />

	* 必选：否 <br />

	* 默认值：aliyun <br />

* **packageAuthorizedProject**

        * 描述：被package授权的project，即用户当前所在project <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **isCompress**

        * 描述：是否压缩读取，bool类型: "true"表示压缩, "false"标示不压缩 <br />

	* 必选：否 <br />

	* 默认值："false" : 不压缩 <br />

### 3.3 类型转换

下面列出 ODPSReader 读出类型与 DataX 内部类型的转换关系：


| ODPS 数据类型| DataX 内部类型    |
| -------- | -----  |
| BIGINT     | Long |
| DOUBLE   | Double |
| STRING   | String |
| DATETIME     | Date |
| Boolean  | Bool  |


## 4 性能报告（线上环境实测）

### 4.1 环境准备

#### 4.1.1 数据特征

建表语句：

    use cdo_datasync;
    create table datax3_odpswriter_perf_10column_1kb_00(
    s_0 string,
    bool_1 boolean,
    bi_2 bigint,
    dt_3 datetime,
    db_4 double,
    s_5 string,
    s_6 string,
    s_7 string,
    s_8 string,
    s_9 string
    )PARTITIONED by (pt string,year string);

单行记录类似于：

    s_0    : 485924f6ab7f272af361cd3f7f2d23e0d764942351#$%^&fdafdasfdas%%^(*&^^&*
    bool_1 : true
    bi_2   : 1696248667889
    dt_3   : 2013-07-0600: 00: 00
    db_4   : 3.141592653578
    s_5    : 100dafdsafdsahofjdpsawifdishaf;dsadsafdsahfdsajf;dsfdsa;fjdsal;11209
    s_6    : 100dafdsafdsahofjdpsawifdishaf;dsadsafdsahfdsajf;dsfdsa;fjdsal;11fdsafdsfdsa209
    s_7    : 100DAFDSAFDSAHOFJDPSAWIFDISHAF;dsadsafdsahfdsajf;dsfdsa;FJDSAL;11209
    s_8    : 100dafdsafdsahofjdpsawifdishaf;DSADSAFDSAHFDSAJF;dsfdsa;fjdsal;11209
    s_9    : 12~!2345100dafdsafdsahofjdpsawifdishaf;dsadsafdsahfdsajf;dsfdsa;fjdsal;11209

#### 4.1.2 机器参数

* 执行DataX的机器参数为:
	1. cpu : 24 Core Intel(R) Xeon(R) CPU E5-2630 0 @ 2.30GHz cache 15.36MB
	2. mem : 50GB
	3. net : 千兆双网卡
	4. jvm : -Xms1024m -Xmx1024m -XX:+HeapDumpOnOutOfMemoryError
	5. disc: DataX 数据不落磁盘，不统计此项

* 任务配置为:
```
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
                    "name": "odpsreader",
                    "parameter": {
                        "accessId": "******************************",
                        "accessKey": "*****************************",
                        "column": [
                            "*"
                        ],
                        "partition": [
                            "pt=20141010000000,year=2014"
                        ],
                        "odpsServer": "http://xxx/api",
                        "project": "cdo_datasync",
                        "table": "datax3_odpswriter_perf_10column_1kb_00",
                        "tunnelServer": "http://xxx"
                    }
                },
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print": false,
                        "column": [
                            {
                                "value": "485924f6ab7f272af361cd3f7f2d23e0d764942351#$%^&fdafdasfdas%%^(*&^^&*"
                            },
                            {
                                "value": "true",
                                "type": "bool"
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
                }
            }
        ]
    }
}
```

### 4.2 测试报告


| 并发任务数| DataX速度(Rec/s)|DataX流量(MB/S)|网卡流量(MB/S)|DataX运行负载|
|--------| --------|--------|--------|--------|
|1|117507|50.20|53.7|0.62|
|2|232976|99.54|108.1|0.99|
|4|387382|165.51|181.3|1.98|
|5|426054|182.03|202.2|2.35|
|6|434793|185.76|204.7|2.77|
|8|495904|211.87|230.2|2.86|
|16|501596|214.31|234.7|2.84|
|32|501577|214.30|234.7|2.99|
|64|501625|214.32|234.7|3.22|

说明：

1. OdpsReader 影响速度最主要的是channel数目，这里到达8时已经打满网卡，过多调大反而会影响系统性能。
2. channel数目的选择，可以考虑odps表文件组织，可尝试合并小文件再进行同步调优。


## 5 约束限制




## FAQ（待补充）

***

**Q: 你来问**

A: 我来答。

***

