# DataX KuduWriter 说明


------------

## 1 快速介绍

KuduWriter提供了Kudu存储系统写入数据的能力，在底层实现上，KuduWriterr将DataX传输协议下的数据转换为Kudu支持的格式，并使用Kudu提供的Java客户端写入到Kudu。

## 2 功能与限制

KuduWriter实现了从DataX协议写入Kudu存储系统，Kudu本身拥有一些限制，约定如下:

1. 不支持Kerberos认证，仅支持Kudu提供的masters方法。

2. 支持Kudu提供的insert和upsert方法

3. 可以控制batch_size。

4. 支持多线程写入。

5. 根据Kudu表的数据类型自动转换DataX内部类型，须保证reader写入的column和KuduWriter保持一致。


## 3 功能说明


### 3.1 配置样例


```json
{
    "setting": {},
    "job": {
        "setting": {
            "speed": {
                "channel": 15
            }
        },
        "content": [
            {
                "reader": {}
                },
                 "writer": {
                    "name": "kuduwriter",
                    "parameter": {
                        "kudu_masters": "ip:port",
                        "tableName": "table_name",
                        "column": [
                            {"name":"col1","type":"STRING"},
                            {"name":"col2","type":"STRING"}
                        ],
                        "batch_size":1024,
                        "isUpsert":false
                    }
                }
          }
        ]
    }
}

```

### 3.2 参数说明

* **kudu_masters**

	* 描述：Kudu集群的master地址 <br />
 
	* 必选：是 <br />
 
	* 默认值：无 <br />

* **tableName**

	* 描述：Kudu集群里已经存在的表名。 <br />
 
	* 必选：是 <br />

	* 默认值：无 <br />
	
* **column**

	* 描述：写入数据的字段，不支持对部分列写入。可以不用指定字段类型，插件会自动检查已存在Kudu表的字段类型。 <br />

		用户可以指定Column字段信息，配置如下：

		```json
		"column":
                 [
                            {
                                "name": "userName",
                                "type": "string"
                            },
                            {
                                "name": "age",
                                "type": "STRING"
                            }
                 ]
		```

	* 必选：是 <br />

	* 默认值：无 <br />

* **batch_size**

	* 描述：每个线程批量插入的数据量 <br />
 
	* 必选：否 <br />
 
	* 默认值：1024 <br />

* **isUpsert**

	* 描述：选择是否支持Upsert方法，默认false <br />
			 
	* 必选：无 <br />
 
	* 默认值：false <br />
 

### 3.3 类型转换

插件会将DataX的类型转换成Kudu支持的数据类型
| Kudu 内部类型| DataX 数据类型    |
| -------- | -----  |
|
| INT8     |Double -> byte|
| INT16   |Double -> short|
| INT32   |Double -> int|
| INT64   |Double -> int|
| UNIXTIME_MICROS   |Long|
| BINARY   |Bytes|
| STRING   |String|
| BOOL   |Boolean|
| FLOAT   |Double -> float|
| DOUBLE   |Double|

其中：

* 建议时间字段转换成String



## 4 性能报告

任务启动时刻                    : 2018-08-08 21:15:33
任务结束时刻                    : 2018-08-09 01:16:24
任务总计耗时                    :              14451s
任务平均流量                    :            5.05MB/s
记录写入速度                    :          11190rec/s
读出记录总数                    :           161714914
读写失败总数                    :                   0

## 5 约束限制

略

## 6 FAQ

略

