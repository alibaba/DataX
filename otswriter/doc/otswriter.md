
# OTSWriter 插件文档


___


## 1 快速介绍

OTSWriter插件实现了向OTS写入数据，目前支持三种写入方式：

* PutRow，对应于OTS API PutRow，插入数据到指定的行，如果该行不存在，则新增一行；若该行存在，则覆盖原有行。

* UpdateRow，对应于OTS API UpdateRow，更新指定行的数据，如果该行不存在，则新增一行；若该行存在，则根据请求的内容在这一行中新增、修改或者删除指定列的值。

* DeleteRow，对应于OTS API DeleteRow，删除指定行的数据。

OTS是构建在阿里云飞天分布式系统之上的 NoSQL数据库服务，提供海量结构化数据的存储和实时访问。OTS 以实例和表的形式组织数据，通过数据分片和负载均衡技术，实现规模上的无缝扩展。

## 2 实现原理

简而言之，OTSWriter通过OTS官方Java SDK连接到OTS服务端，并通过SDK写入OTS服务端。OTSWriter本身对于写入过程做了很多优化，包括写入超时重试、异常写入重试、批量提交等Feature。


## 3 功能说明

### 3.1 配置样例

* 配置一个写入OTS作业:

```
{
    "job": {
        "setting": {
        },
        "content": [
            {
                "reader": {},
                "writer": {
                    "name": "otswriter",
                    "parameter": {
                        "endpoint":"",
                        "accessId":"",
                        "accessKey":"",
                        "instanceName":"",
                        // 导出数据表的表名
                        "table":"",

                        // Writer支持不同类型之间进行相互转换
                        // 如下类型转换不支持:
                        // ================================
                        //    int    -> binary
                        //    double -> bool, binary
                        //    bool   -> binary
                        //    bytes  -> int, double, bool
                        // ================================

                        // 需要导入的PK列名，区分大小写
                        // 类型支持：STRING，INT
                        // 1. 支持类型转换，注意类型转换时的精度丢失
                        // 2. 顺序不要求和表的Meta一致
                        "primaryKey" : [
                            {"name":"pk1", "type":"string"},
                            {"name":"pk2", "type":"int"}
                        ],

                        // 需要导入的列名，区分大小写
                        // 类型支持STRING，INT，DOUBLE，BOOL和BINARY
                        "column" : [
                            {"name":"col2", "type":"INT"},
                            {"name":"col3", "type":"STRING"},
                            {"name":"col4", "type":"STRING"},
                            {"name":"col5", "type":"BINARY"},
                            {"name":"col6", "type":"DOUBLE"}
                        ],

                        // 写入OTS的方式
                        // PutRow : 等同于OTS API中PutRow操作，检查条件是ignore
                        // UpdateRow : 等同于OTS API中UpdateRow操作，检查条件是ignore
                        // DeleteRow: 等同于OTS API中DeleteRow操作，检查条件是ignore
                        "writeMode" : "PutRow"
                    }
                }
            }
        ]
    }
}
```


### 3.2 参数说明

* **endpoint**

	* 描述：OTS Server的EndPoint(服务地址)，例如http://bazhen.cn−hangzhou.ots.aliyuncs.com。

	* 必选：是 <br />

	* 默认值：无 <br />

* **accessId**

	* 描述：OTS的accessId <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **accessKey**

	* 描述：OTS的accessKey  <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **instanceName**

	* 描述：OTS的实例名称，实例是用户使用和管理 OTS 服务的实体，用户在开通 OTS 服务之后，需要通过管理控制台来创建实例，然后在实例内进行表的创建和管理。实例是 OTS 资源管理的基础单元，OTS 对应用程序的访问控制和资源计量都在实例级别完成。 <br />

	* 必选：是 <br />

	* 默认值：无 <br />


* **table**

	* 描述：所选取的需要抽取的表名称，这里有且只能填写一张表。在OTS不存在多表同步的需求。<br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **primaryKey**

	* 描述: OTS的主键信息，使用JSON的数组描述字段信息。OTS本身是NoSQL系统，在OTSWriter导入数据过程中，必须指定相应地字段名称。

		OTS的PrimaryKey只能支持STRING，INT两种类型，因此OTSWriter本身也限定填写上述两种类型。

		DataX本身支持类型转换的，因此对于源头数据非String/Int，OTSWriter会进行数据类型转换。

		配置实例:

		```json
		"primaryKey" : [
                            {"name":"pk1", "type":"string"},
                            {"name":"pk2", "type":"int"}
                        ],
		```
	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：所配置的表中需要同步的列名集合，使用JSON的数组描述字段信息。使用格式为

		```json
			{"name":"col2", "type":"INT"},
		```

		其中的name指定写入的OTS列名，type指定写入的类型。OTS类型支持STRING，INT，DOUBLE，BOOL和BINARY几种类型 。

		写入过程不支持常量、函数或者自定义表达式。

	* 必选：是 <br />

	* 默认值：无 <br />

* **writeMode**

	* 描述：写入模式，目前支持两种模式，

		* PutRow，对应于OTS API PutRow，插入数据到指定的行，如果该行不存在，则新增一行；若该行存在，则覆盖原有行。

		* UpdateRow，对应于OTS API UpdateRow，更新指定行的数据，如果该行不存在，则新增一行；若该行存在，则根据请求的内容在这一行中新增、修改或者删除指定列的值。

		* DeleteRow，对应于OTS API DeleteRow，删除指定行的数据。

	* 必选：是 <br />

	* 默认值：无 <br />


### 3.3 类型转换

目前OTSWriter支持所有OTS类型，下面列出OTSWriter针对OTS类型转换列表:


| DataX 内部类型| OTS 数据类型    |
| -------- | -----  |
| Long     |Integer |
| Double   |Double|
| String   |String|
| Boolean  |Boolean|
| Bytes    |Binary |

* 注意，OTS本身不支持日期型类型。应用层一般使用Long报错时间的Unix TimeStamp。

## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征

2列PK（10 + 8），15列String(10 Byte), 2两列Integer(8 Byte)，算上Column Name每行大概327Byte，每次BatchWriteRow写入100行数据，所以当个请求的数据大小是32KB。

#### 4.1.2 机器参数

OTS端：3台前端机，5台后端机

DataX运行端: 24核CPU， 98GB内存

### 4.2 测试报告

#### 4.2.1 测试报告

|并发数|DataX CPU|DATAX流量 |OTS 流量 | BatchWrite前端QPS| BatchWriteRow前端延时|
|--------|--------| --------|--------|--------|------|
|40| 1027% |Speed 22.13MB/s, 112640 records/s|65.8M/s |42|153ms |
|50| 1218% |Speed 24.11MB/s, 122700 records/s|73.5M/s |47|174ms|
|60| 1355% |Speed 25.31MB/s, 128854 records/s|78.1M/s |50|190ms|
|70| 1578% |Speed 26.35MB/s, 134121 records/s|80.8M/s |52|210ms|
|80| 1771% |Speed 26.55MB/s, 135161 records/s|82.7M/s |53|230ms|




## 5 约束限制

### 5.1 写入幂等性

OTS写入本身是支持幂等性的，也就是使用OTS SDK同一条数据写入OTS系统，一次和多次请求的结果可以理解为一致的。因此对于OTSWriter多次尝试写入同一条数据与写入一条数据结果是等同的。

### 5.2 单任务FailOver

由于OTS写入本身是幂等性的，因此可以支持单任务FailOver。即一旦写入Fail，DataX会重新启动相关子任务进行重试。

## 6 FAQ
