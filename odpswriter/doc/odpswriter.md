# DataX ODPS写入


---


## 1 快速介绍

ODPSWriter插件用于实现往ODPS插入或者更新数据，主要提供给etl开发同学将业务数据导入odps，适合于TB,GB数量级的数据传输，如果需要传输PB量级的数据，请选择dt task工具 ;



## 2 实现原理

在底层实现上，ODPSWriter是通过DT Tunnel写入ODPS系统的，有关ODPS的更多技术细节请参看 ODPS主站 https://data.aliyun.com/product/odps 和ODPS产品文档 https://help.aliyun.com/product/27797.html

目前 DataX3 依赖的 SDK 版本是：

                    <dependency>
                        <groupId>com.aliyun.odps</groupId>
                        <artifactId>odps-sdk-core-internal</artifactId>
                        <version>0.13.2</version>
                    </dependency>


注意: **如果你需要使用ODPSReader/Writer插件，请务必使用JDK 1.6-32及以上版本**
使用java -version查看Java版本号

## 3 功能说明

### 3.1 配置样例
* 这里使用一份从内存产生到ODPS导入的数据。

```json
{
    "job": {
        "setting": {
            "speed": {"byte": 1048576}
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
                                "value": "test",
                                "type": "bytes"
                            }
                        ],
                        "sliceRecordCount": 100000
                    }
                },
                  "writer": {
                     "name": "odpswriter",
                        "parameter": {
                          "project": "chinan_test",
                          "table": "odps_write_test00_partitioned",
                          "partition":"school=SiChuan-School,class=1",
                          "column": ["id","name"],
                          "accessId": "xxx",
                          "accessKey": "xxxx",
                          "truncate": true,
                          "odpsServer": "http://sxxx/api",
                          "tunnelServer": "http://xxx",
                          "accountType": "aliyun"
                       }
                    }
                }
            }
        ]
    }
}
```


### 3.2 参数说明


* **accessId**
	* 描述：ODPS系统登录ID <br />
 	* 必选：是 <br />
 	* 默认值：无 <br />

* **accessKey**
	* 描述：ODPS系统登录Key <br />
 	* 必选：是 <br />
 	* 默认值：无 <br />

* **project**

	* 描述：ODPS表所属的project，注意:Project只能是字母+数字组合，请填写英文名称。在云端等用户看到的ODPS项目中文名只是显示名，请务必填写底层真实地Project英文标识名。<br />
	* 必选：是 <br />
 	* 默认值：无 <br />

* **table**

 	* 描述：写入数据的表名，不能填写多张表，因为DataX不支持同时导入多张表。 <br />
 	* 必选：是 <br />
 	* 默认值：无 <br />

* **partition**

	* 描述：需要写入数据表的分区信息，必须指定到最后一级分区。把数据写入一个三级分区表，必须配置到最后一级分区，例如pt=20150101/type＝1/biz=2。
	 <br />
	* 必选：**如果是分区表，该选项必填，如果非分区表，该选项不可填写。**
	* 默认值：空 <br />

* **column**

	* 描述：需要导入的字段列表，当导入全部字段时，可以配置为"column": ["*"], 当需要插入部分odps列填写部分列，例如"column": ["id", "name"]。ODPSWriter支持列筛选、列换序，例如表有a,b,c三个字段，用户只同步c,b两个字段。可以配置成["c","b"], 在导入过程中，字段a自动补空，设置为null。 <br />
	* 必选：否 <br />
	* 默认值：无 <br />

* **truncate**
	* 描述：ODPSWriter通过配置"truncate": true，保证写入的幂等性，即当出现写入失败再次运行时，ODPSWriter将清理前述数据，并导入新数据，这样可以保证每次重跑之后的数据都保持一致。 <br />

		**truncate选项不是原子操作！ODPS SQL无法做到原子性。因此当多个任务同时向一个Table/Partition清理分区时候，可能出现并发时序问题，请务必注意！**针对这类问题，我们建议尽量不要多个作业DDL同时操作同一份分区，或者在多个并发作业启动前，提前创建分区。

 	* 必选：是 <br />
 	* 默认值：无 <br />

* **odpsServer**

	* 描述：ODPS的server地址，线上地址为 http://service.odps.aliyun.com/api <br />
	* 必选：是 <br />
	* 默认值：无 <br />

* **tunnelServer**

	* 描述：ODPS的tunnelserver地址，线上地址为 http://dt.odps.aliyun.com  <br />
	* 必选：是， <br />
	* 默认值：无 <br />


### 3.3 类型转换

类似ODPSReader，目前ODPSWriter支持大部分ODPS类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出ODPSWriter针对ODPS类型转换列表:


| DataX 内部类型| ODPS 数据类型    |
| -------- | ----- |
| Long     |bigint |
| Double   |double |
| String   |string |
| Date     |datetime |
| Boolean  |bool |




##  4  插件特点

### 4.1  关于列筛选的问题

* ODPS本身不支持列筛选、重排序、补空等等，但是DataX ODPSWriter完成了上述需求，支持列筛选、重排序、补空。例如需要导入的字段列表，当导入全部字段时，可以配置为"column": ["*"]，odps表有a,b,c三个字段，用户只同步c,b两个字段，在列配置中可以写成"column": ["c","b"]，表示会把reader的第一列和第二列导入odps的c字段和b字段，而odps表中新插入记录的a字段会被置为null.

### 4.2  列配置错误的处理

* 为了保证写入数据的可靠性，避免多余列数据丢失造成数据质量故障。对于写入多余的列，ODPSWriter将报错。例如ODPS表字段为a,b,c，但是ODPSWriter写入的字段为多于3列的话ODPSWriter将报错。

### 4.3  分区配置注意事项

* ODPSWriter只提供 **写入到最后一级分区** 功能，不支持写入按照某个字段进行分区路由等功能。假设表一共有3级分区，那么在分区配置中就必须指明写入到某个三级分区，例如把数据写入一个表的第三级分区，可以配置为 pt=20150101/type＝1/biz=2，但是不能配置为pt=20150101/type＝1或者pt=20150101。

### 4.4  任务重跑和failover
* ODPSWriter通过配置"truncate": true，保证写入的幂等性，即当出现写入失败再次运行时，ODPSWriter将清理前述数据，并导入新数据，这样可以保证每次重跑之后的数据都保持一致。如果在运行过程中因为其他的异常导致了任务中断，是不能保证数据的原子性的，数据不会回滚也不会自动重跑，需要用户利用幂等性这一特点重跑去确保保证数据的完整性。**truncate为true的情况下，会将指定分区\表的数据全部清理，请谨慎使用！**



## 5 性能报告（线上环境实测）

### 5.1 环境准备

#### 5.1.1 数据特征

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

#### 5.1.2 机器参数

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
                "channel": "1,2,4,5,6,8,16,32,64"
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
                        "odpsServer": "http://service.odps.aliyun.com/api",
                        "project": "cdo_datasync",
                        "table": "datax3_odpswriter_perf_10column_1kb_00",
                        "tunnelServer": "http://dt.odps.aliyun.com"
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

### 5.2 测试报告


| 并发任务数|blockSizeInMB| DataX速度(Rec/s)|DataX流量(MB/S)|网卡流量(MB/S)|DataX运行负载|
|--------| --------|--------|--------|--------|--------|
|1|32|30303|13.03|14.5|0.12|
|1|64|38461|16.54|16.5|0.44|
|1|128|46454|20.55|26.7|0.47|
|1|256|52631|22.64|26.7|0.47|
|1|512|58823|25.30|28.7|0.44|
|4|32|114816|49.38|55.3|0.75|
|4|64|147577|63.47|71.3|0.82|
|4|128|177744|76.45|83.2|0.97|
|4|256|173913|74.80|80.1|1.01|
|4|512|200000|86.02|95.1|1.41|
|8|32|204480|87.95|92.7|1.16|
|8|64|294224|126.55|135.3|1.65|
|8|128|365475|157.19|163.7|2.89|
|8|256|394713|169.83|176.7|2.72|
|8|512|241691|103.95|125.7|2.29|
|16|32|420838|181.01|198.0|2.56|
|16|64|458144|197.05|217.4|2.85|
|16|128|443219|190.63|210.5|3.29|
|16|256|315235|135.58|140.0|0.95|
|16|512|OOM|||||

说明：

1. OdpsWriter 影响速度的是channel 和 blockSizeInMB。blockSizeInMB 取`32` 和 `64`时，速度比较稳定,过分大的 blockSizeInMB 可能造成速度波动以及内存OOM。
2. channel 和 blockSizeInMB 对速度的影响都很明显，建议综合考虑配合选择。
3. channel 数目的选择，可以综合考虑源端数据特征进行选择，对于StreamReader，在16个channel时将网卡打满。


## 6 FAQ
#### 1  导数据到 odps 的日志中有以下报错，该怎么处理呢？"ODPS-0420095: Access Denied - Authorization Failed [4002], You doesn‘t exist in project example_dev“

解决办法 :找ODPS Prject 的 owner给用户的云账号授权，授权语句：
grant Describe,Select,Alter,Update on table [tableName] to user XXX

#### 2  可以导入数据到odps的视图吗？
目前不支持通过视图到数据到odps,视图是ODPS非实体化数据存储对象，技术上无法向视图导入数据。
