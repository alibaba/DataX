
# OTSReader 插件文档


___


## 1 快速介绍

OTSReader插件实现了从OTS读取数据，并可以通过用户指定抽取数据范围可方便的实现数据增量抽取的需求。目前支持三种抽取方式：

* 全表抽取
* 范围抽取
* 指定分片抽取

OTS是构建在阿里云飞天分布式系统之上的 NoSQL数据库服务，提供海量结构化数据的存储和实时访问。OTS 以实例和表的形式组织数据，通过数据分片和负载均衡技术，实现规模上的无缝扩展。

## 2 实现原理

简而言之，OTSReader通过OTS官方Java SDK连接到OTS服务端，获取并按照DataX官方协议标准转为DataX字段信息传递给下游Writer端。

OTSReader会根据OTS的表范围，按照Datax并发的数目N，将范围等分为N份Task。每个Task都会有一个OTSReader线程来执行。

## 3 功能说明

### 3.1 配置样例

* 配置一个从OTS全表同步抽取数据到本地的作业:

```
{
    "job": {
        "setting": {
        },
        "content": [
            {
                "reader": {
                    "name": "otsreader",
                    "parameter": {
                        /* ----------- 必填 --------------*/
                        "endpoint":"",
                        "accessId":"",
                        "accessKey":"",
                        "instanceName":"",

                        // 导出数据表的表名
                        "table":"",

                        // 需要导出的列名，支持重复列和常量列，区分大小写
                        // 常量列：类型支持STRING，INT，DOUBLE，BOOL和BINARY
                        // 备注：BINARY需要通过Base64转换为对应的字符串传入插件
                        "column":[
                            {"name":"col1"},                          // 普通列
                            {"name":"col2"},                          // 普通列
                            {"name":"col3"},                          // 普通列
                            {"type":"STRING", "value" : "bazhen"},    // 常量列(字符串)
                            {"type":"INT", "value" : ""},              // 常量列(整形)
                            {"type":"DOUBLE", "value" : ""},           // 常量列(浮点)
                            {"type":"BOOL", "value" : ""},             // 常量列(布尔)
                            {"type":"BINARY", "value" : "Base64(bin)"} // 常量列(二进制),使用Base64编码完成
                        ],
                        "range":{
                            // 导出数据的起始范围
                            // 支持INF_MIN, INF_MAX, STRING, INT
                            "begin":[
                                  {"type":"INF_MIN"},
                            ],
                            // 导出数据的结束范围
                            // 支持INF_MIN, INF_MAX, STRING, INT
                            "end":[
                                  {"type":"INF_MAX"},
                            ]
                        }
                    }
                },
                "writer": {}
            }
        ]
    }
}
```

* 配置一个定义抽取范围的OTSReader：

```
{
    "job": {
        "setting": {
            "speed": {
                "byte":10485760
            },
            "errorLimit":0.0
        },
        "content": [
            {
                "reader": {
                    "name": "otsreader",
                    "parameter": {
                        "endpoint":"",
                        "accessId":"",
                        "accessKey":"",
                        "instanceName":"",

                        // 导出数据表的表名
                        "table":"",

                        // 需要导出的列名，支持重复类和常量列，区分大小写
                        // 常量列：类型支持STRING，INT，DOUBLE，BOOL和BINARY
                        // 备注：BINARY需要通过Base64转换为对应的字符串传入插件
                        "column":[
                            {"name":"col1"},                          // 普通列
                            {"name":"col2"},                          // 普通列
                            {"name":"col3"},                          // 普通列
                            {"type":"STRING","value" : ""},           // 常量列(字符串)
                            {"type":"INT","value" : ""},              // 常量列(整形)
                            {"type":"DOUBLE","value" : ""},           // 常量列(浮点)
                            {"type":"BOOL","value" : ""},             // 常量列(布尔)
                            {"type":"BINARY","value" : "Base64(bin)"} // 常量列(二进制)
                        ],
                        "range":{
                            // 导出数据的起始范围
                            // 支持INF_MIN, INF_MAX, STRING, INT
                            "begin":[
                                {"type":"INF_MIN"},
                                {"type":"INF_MAX"},
                                {"type":"STRING", "value":"hello"},
                                {"type":"INT", "value":"2999"},
                            ],
                            // 导出数据的结束范围
                            // 支持INF_MIN, INF_MAX, STRING, INT
                            "end":[
                                {"type":"INF_MAX"},
                                {"type":"INF_MIN"},
                                {"type":"STRING", "value":"hello"},
                                {"type":"INT", "value":"2999"},
                            ]
                        }
                    }
                },
                "writer": {}
            }
        ]
    }
}
```


### 3.2 参数说明

* **endpoint**

	* 描述：OTS Server的EndPoint地址，例如http://bazhen.cn−hangzhou.ots.aliyuncs.com。

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

* **column**

	* 描述：所配置的表中需要同步的列名集合，使用JSON的数组描述字段信息。由于OTS本身是NoSQL系统，在OTSReader抽取数据过程中，必须指定相应地字段名称。

		支持普通的列读取，例如: {"name":"col1"}

		支持部分列读取，如用户不配置该列，则OTSReader不予读取。

		支持常量列读取，例如: {"type":"STRING", "value" : "DataX"}。使用type描述常量类型，目前支持STRING、INT、DOUBLE、BOOL、BINARY(用户使用Base64编码填写)、INF_MIN(OTS的系统限定最小值，使用该值用户不能填写value属性，否则报错)、INF_MAX(OTS的系统限定最大值，使用该值用户不能填写value属性，否则报错)。

		不支持函数或者自定义表达式，由于OTS本身不提供类似SQL的函数或者表达式功能，OTSReader也不能提供函数或表达式列功能。

	* 必选：是 <br />

	* 默认值：无 <br />

* **begin/end**

	* 描述：该配置项必须配对使用，用于支持OTS表范围抽取。begin/end中描述的是OTS **PrimaryKey**的区间分布状态，而且必须保证区间覆盖到所有的PrimaryKey，**需要指定该表下所有的PrimaryKey范围，不能遗漏任意一个PrimaryKey**，对于无限大小的区间，可以使用{"type":"INF_MIN"}，{"type":"INF_MAX"}指代。例如对一张主键为 [DeviceID, SellerID]的OTS进行抽取任务，begin/end可以配置为:

	```json
		"range": {
			"begin": {
				{"type":"INF_MIN"},  //指定deviceID最小值
				{"type":"INT", "value":"0"}  //指定deviceID最小值
			},
			"end": {
				{"type":"INF_MAX"}, //指定deviceID抽取最大值
				{"type":"INT", "value":"9999"} //指定deviceID抽取最大值
			}
		}
	```


	   如果要对上述表抽取全表，可以使用如下配置：

	```
		"range": {
			"begin": [
				{"type":"INF_MIN"},  //指定deviceID最小值
				{"type":"INF_MIN"} //指定SellerID最小值
			],
			"end": [
				{"type":"INF_MAX"}, //指定deviceID抽取最大值
		    	{"type":"INF_MAX"} //指定SellerID抽取最大值
			]
		}
	```

	* 必选：是 <br />

	* 默认值：空 <br />

* **split**

	* 描述：该配置项属于高级配置项，是用户自己定义切分配置信息，普通情况下不建议用户使用。适用场景通常在OTS数据存储发生热点，使用OTSReader自动切分的策略不能生效情况下，使用用户自定义的切分规则。split指定是的在Begin、End区间内的切分点，且只能是partitionKey的切分点信息，即在split仅配置partitionKey，而不需要指定全部的PrimaryKey。

		例如对一张主键为 [DeviceID, SellerID]的OTS进行抽取任务，可以配置为:

	```json
		"range": {
			"begin": {
				{"type":"INF_MIN"},  //指定deviceID最小值
				{"type":"INF_MIN"}  //指定deviceID最小值
			},
			"end": {
				{"type":"INF_MAX"}, //指定deviceID抽取最大值
				{"type":"INF_MAX"} //指定deviceID抽取最大值
			}，
			 // 用户指定的切分点，如果指定了切分点，Job将按照begin、end和split进行Task的切分，
            // 切分的列只能是Partition Key（ParimaryKey的第一列）
            // 支持INF_MIN, INF_MAX, STRING, INT
            "split":[
                                {"type":"STRING", "value":"1"},
                                {"type":"STRING", "value":"2"},
                                {"type":"STRING", "value":"3"},
                                {"type":"STRING", "value":"4"},
                                {"type":"STRING", "value":"5"}
                    ]
		}
	```

	* 必选：否 <br />

	* 默认值：无 <br />


### 3.3 类型转换

目前OTSReader支持所有OTS类型，下面列出OTSReader针对OTS类型转换列表:


| DataX 内部类型| OTS 数据类型    |
| -------- | -----  |
| Long     |Integer |
| Double   |Double|
| String   |String|
| Boolean  |Boolean |
| Bytes    |Binary |


* 注意，OTS本身不支持日期型类型。应用层一般使用Long报错时间的Unix TimeStamp。

## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征

15列String(10 Byte), 2两列Integer(8 Byte)，总计168Byte/r。

#### 4.1.2 机器参数

OTS端：3台前端机，5台后端机

DataX运行端: 24核CPU， 98GB内存

#### 4.1.3 DataX jvm 参数

	-Xms1024m -Xmx1024m -XX:+HeapDumpOnOutOfMemoryError

### 4.2 测试报告

#### 4.2.1 测试报告

|并发数|DataX CPU|OTS 流量|DATAX流量 | 前端QPS| 前端延时|
|--------|--------| --------|--------|--------|------|
|2|	36%	|6.3M/s	|12739 rec/s | 4.7 | 308ms |
|11| 155% |	32M/s |60732 rec/s | 23.9 |	412ms |
|50| 377% |	73M/s |145139 rec/s	| 54 |	874ms |
|100| 448% | 82M/s | 156262 rec/s |60 |	1570ms |



## 5 约束限制

### 5.1 一致性约束

OTS是类BigTable的存储系统，OTS本身能够保证单行写事务性，无法提供跨行级别的事务。对于OTSReader而言也无法提供全表的一致性视图。例如对于OTSReader在0点启动的数据同步任务，在整个表数据同步过程中，OTSReader同样会抽取到后续更新的数据，无法提供准确的0点时刻该表一致性视图。

### 5.2 增量数据同步

OTS本质上KV存储，目前只能针对PK进行范围查询，暂不支持按照字段范围抽取数据。因此只能对于增量查询，如果PK能够表示范围信息，例如自增ID，或者时间戳。

自增ID，OTSReader可以通过记录上次最大的ID信息，通过指定Range范围进行增量抽取。这样使用的前提是OTS中的PrimaryKey必须包含主键自增列(自增主键需要使用OTS应用方生成。)

时间戳，	OTSReader可以通过PK过滤时间戳，通过制定Range范围进行增量抽取。这样使用的前提是OTS中的PrimaryKey必须包含主键时间列(时间主键需要使用OTS应用方生成。)

## 6 FAQ

