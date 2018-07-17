# Hbase094XWriter & Hbase11XWriter 插件文档

___

## 1 快速介绍

HbaseWriter 插件实现了从向Hbase中写取数据。在底层实现上，HbaseWriter 通过 HBase 的 Java 客户端连接远程 HBase 服务，并通过 put 方式写入Hbase。  


### 1.1支持功能

1、目前HbaseWriter支持的Hbase版本有：Hbase0.94.x和Hbase1.1.x。

* 若您的hbase版本为Hbase0.94.x，writer端的插件请选择：hbase094xwriter，即：

	```
	"writer": {
          "name": "hbase094xwriter"
      }
	```

* 若您的hbase版本为Hbase1.1.x，writer端的插件请选择：hbase11xwriter,即：
	
	```
	"writer": {
          "name": "hbase11xwriter"
      }
	```

2、目前HbaseWriter支持源端多个字段拼接作为hbase 表的 rowkey，具体配置参考：rowkeyColumn配置；

3、写入hbase的时间戳（版本）支持：用当前时间作为版本，指定源端列作为版本，指定一个时间 三种方式作为版本；

4、HbaseWriter中有一个必填配置项是：hbaseConfig，需要你联系 HBase PE，将hbase-site.xml 中与连接 HBase 相关的配置项提取出来，以 json 格式填入，同时可以补充更多HBase client的配置来优化与服务器的交互。   


如：hbase-site.xml的配置内容如下

```
<configuration>
  <property>
    <name>hbase.rootdir</name>
    <value>hdfs://ip:9000/hbase</value>
  </property>
  <property>
    <name>hbase.cluster.distributed</name>
    <value>true</value>
  </property>
  <property>
    <name>hbase.zookeeper.quorum</name>
    <value>***</value>
  </property>
</configuration>
```
转换后的json为：

```
"hbaseConfig": {
              "hbase.rootdir": "hdfs: //ip: 9000/hbase",
              "hbase.cluster.distributed": "true",
              "hbase.zookeeper.quorum": "***"
            }
```

### 1.2 限制

1、目前只支持源端为横表写入，不支持竖表（源端读出的为四元组: rowKey，family:qualifier，timestamp，value）模式的数据写入；本期目标主要是替换DataX2中的habsewriter，下次迭代考虑支持。

2、目前不支持写入hbase前清空表数据，若需要清空数据请联系HBase PE

## 2 实现原理

简而言之，HbaseWriter 通过 HBase 的 Java 客户端，通过 HTable, Put等 API，将从上游Reader读取的数据写入HBase你hbase11xwriter与hbase094xwriter的主要不同在于API的调用不同，Hbase1.1.x废弃了很多Hbase0.94.x的api。



## 3 功能说明

### 3.1 配置样例

* 配置一个从本地写入hbase1.1.x的作业：

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
          "name": "txtfilereader",
          "parameter": {
            "path": "/Users/shf/workplace/datax_test/hbase11xwriter/txt/normal.txt",
            "charset": "UTF-8",
            "column": [
              {
                "index": 0,
                "type": "String"
              },
              {
                "index": 1,
                "type": "string"
              },
              {
                "index": 2,
                "type": "string"
              },
              {
                "index": 3,
                "type": "string"
              },
              {
                "index": 4,
                "type": "string"
              },
              {
                "index": 5,
                "type": "string"
              },
              {
                "index": 6,
                "type": "string"
              }

            ],
            "fieldDelimiter": ","
          }
        },
        "writer": {
          "name": "hbase11xwriter",
          "parameter": {
            "hbaseConfig": {
              "hbase.rootdir": "hdfs: //ip: 9000/hbase",
              "hbase.cluster.distributed": "true",
              "hbase.zookeeper.quorum": "***"
            },
            "table": "writer",
            "mode": "normal",
            "rowkeyColumn": [
                {
                  "index":0,
                  "type":"string"
                },
                {
                  "index":-1,
                  "type":"string",
                  "value":"_"
                }
            ],
            "column": [
              {
                "index":1,
                "name": "cf1:q1",
                "type": "string"
              },
              {
                "index":2,
                "name": "cf1:q2",
                "type": "string"
              },
              {
                "index":3,
                "name": "cf1:q3",
                "type": "string"
              },
              {
                "index":4,
                "name": "cf2:q1",
                "type": "string"
              },
              {
                "index":5,
                "name": "cf2:q2",
                "type": "string"
              },
              {
                "index":6,
                "name": "cf2:q3",
                "type": "string"
              }
            ],
            "versionColumn":{
              "index": -1,
              "value":"123456789"
            },
            "encoding": "utf-8"
          }
        }
      }
    ]
  }
}
```


### 3.2 参数说明

* **hbaseConfig**

	* 描述：每个HBase集群提供给DataX客户端连接的配置信息存放在hbase-site.xml，请联系你的HBase PE提供配置信息，并转换为JSON格式。同时可以补充更多HBase client的配置，如：设置scan的cache、batch来优化与服务器的交互。
 
	* 必选：是 <br />
 
	* 默认值：无 <br />
 
* **mode**
 
	* 描述：写hbase的模式，目前只支持normal 模式，后续考虑动态列模式<br />
 
	* 必选：是 <br />
 
	* 默认值：无 <br />
	
* **table**
 
	* 描述：要写的 hbase 表名（大小写敏感） <br />
 
	* 必选：是 <br />
 
	* 默认值：无 <br />

* **encoding**

	* 描述：编码方式，UTF-8 或是 GBK，用于 String 转 HBase byte[]时的编码 <br />
 
	* 必选：否 <br />
 
	* 默认值：UTF-8 <br />
 
  
* **column**

	* 描述：要写入的hbase字段。index：指定该列对应reader端column的索引，从0开始；name：指定hbase表中的列，必须为 列族:列名 的格式；type：指定写入数据类型，用于转换HBase byte[]。配置格式如下：
	
	```
"column": [
              {
                "index":1,
                "name": "cf1:q1",
                "type": "string"
              },
              {
                "index":2,
                "name": "cf1:q2",
                "type": "string"
              }
           ］ 	
		            
	```

	* 必选：是<br />
 
	* 默认值：无 <br />

* **rowkeyColumn**

	* 描述：要写入的hbase的rowkey列。index：指定该列对应reader端column的索引，从0开始，若为常量index为－1；type：指定写入数据类型，用于转换HBase byte[]；value：配置常量，常作为多个字段的拼接符。hbasewriter会将rowkeyColumn中所有列按照配置顺序进行拼接作为写入hbase的rowkey，不能全为常量。配置格式如下：
	
	```
"rowkeyColumn": [
                {
                  "index":0,
                  "type":"string"
                },
                {
                  "index":-1,
                  "type":"string",
                  "value":"_"
                }
            ] 	
		            
	```

	* 必选：是<br />
 
	* 默认值：无 <br />
	
* **versionColumn**

	* 描述：指定写入hbase的时间戳。支持：当前时间、指定时间列，指定时间，三者选一。若不配置表示用当前时间。index：指定对应reader端column的索引，从0开始，需保证能转换为long,若是Date类型，会尝试用yyyy-MM-dd HH:mm:ss和yyyy-MM-dd HH:mm:ss SSS去解析；若为指定时间index为－1；value：指定时间的值,long值。配置格式如下：
	
	```
"versionColumn":{
	"index":1
}
		            
	```
	
	或者
	
	```
"versionColumn":{
	"index":－1,
	"value":123456789
}
		            
	```

	* 必选：否<br />
 
	* 默认值：无 <br />
	

* **nullMode**

	* 描述：读取的null值时，如何处理。支持两种方式：（1）skip：表示不向hbase写这列；（2）empty：写入HConstants.EMPTY_BYTE_ARRAY，即new byte [0] <br />
	  
	* 必选：否<br />
 
	* 默认值：skip<br />	

* **walFlag**

	* 描述：在HBae client向集群中的RegionServer提交数据时（Put/Delete操作），首先会先写WAL（Write Ahead Log）日志（即HLog，一个RegionServer上的所有Region共享一个HLog），只有当WAL日志写成功后，再接着写MemStore，然后客户端被通知提交数据成功；如果写WAL日志失败，客户端则被通知提交失败。关闭（false）放弃写WAL日志，从而提高数据写入的性能。<br />
	  
	* 必选：否<br />
 
	* 默认值：false<br />

* **writeBufferSize**

	* 描述：设置HBae client的写buffer大小，单位字节。配合autoflush使用。autoflush，开启（true）表示Hbase client在写的时候有一条put就执行一次更新；关闭（false），表示Hbase client在写的时候只有当put填满客户端写缓存时，才实际向HBase服务端发起写请求<br />
	  
	* 必选：否<br />
 
	* 默认值：8M<br />

### 3.3 HBase支持的列类型
* BOOLEAN
* SHORT
* INT
* LONG
* FLOAT
* DOUBLE
* STRING



请注意:

* `除上述罗列字段类型外，其他类型均不支持`。

## 4 性能报告

略

## 5 约束限制

略

## 6 FAQ

***
