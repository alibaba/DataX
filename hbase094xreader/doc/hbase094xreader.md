# Hbase094XReader & Hbase11XReader 插件文档

___

## 1 快速介绍

HbaseReader 插件实现了从 Hbase中读取数据。在底层实现上，HbaseReader 通过 HBase 的 Java 客户端连接远程 HBase 服务，并通过 Scan 方式读取你指定 rowkey 范围内的数据，并将读取的数据使用 DataX 自定义的数据类型拼装为抽象的数据集，并传递给下游 Writer 处理。


### 1.1支持的功能

1、目前HbaseReader支持的Hbase版本有：Hbase0.94.x和Hbase1.1.x。

* 若您的hbase版本为Hbase0.94.x，reader端的插件请选择：hbase094xreader，即：

	```
	"reader": {
          "name": "hbase094xreader"
      }
	```

* 若您的hbase版本为Hbase1.1.x，reader端的插件请选择：hbase11xreader,即：
	
	```
	"reader": {
          "name": "hbase11xreader"
      }
	```

2、目前HbaseReader支持两模式读取：normal 模式、multiVersionFixedColumn模式；

* normal 模式：把HBase中的表，当成普通二维表（横表）进行读取,读取最新版本数据。如：

	```
hbase(main):017:0> scan 'users'
ROW                                   COLUMN+CELL
 lisi                                 column=address:city, timestamp=1457101972764, value=beijing
 lisi                                 column=address:contry, timestamp=1457102773908, value=china
 lisi                                 column=address:province, timestamp=1457101972736, value=beijing
 lisi                                 column=info:age, timestamp=1457101972548, value=27
 lisi                                 column=info:birthday, timestamp=1457101972604, value=1987-06-17
 lisi                                 column=info:company, timestamp=1457101972653, value=baidu
 xiaoming                             column=address:city, timestamp=1457082196082, value=hangzhou
 xiaoming                             column=address:contry, timestamp=1457082195729, value=china
 xiaoming                             column=address:province, timestamp=1457082195773, value=zhejiang
 xiaoming                             column=info:age, timestamp=1457082218735, value=29
 xiaoming                             column=info:birthday, timestamp=1457082186830, value=1987-06-17
 xiaoming                             column=info:company, timestamp=1457082189826, value=alibaba
2 row(s) in 0.0580 seconds
```
读取后数据

	| rowKey    | addres:city          | address:contry   | address:province | 	info:age| info:birthday    | info:company           | 
	| --------| ---------------- |-----     |-----  |--------| ---------------- |-----     |
| lisi  | beijing|  china| beijing |27 | 1987-06-17 | baidu|
| xiaoming  | hangzhou| china | zhejiang|29 | 1987-06-17 | alibaba|



* multiVersionFixedColumn模式：把HBase中的表，当成竖表进行读取。读出的每条记录一定是四列形式，依次为：rowKey，family:qualifier，timestamp，value。读取时需要明确指定要读取的列，把每一个 cell 中的值，作为一条记录（record），若有多个版本就有多条记录（record）。如：

	```
hbase(main):018:0> scan 'users',{VERSIONS=>5}
ROW                                   COLUMN+CELL
 lisi                                 column=address:city, timestamp=1457101972764, value=beijing
 lisi                                 column=address:contry, timestamp=1457102773908, value=china
 lisi                                 column=address:province, timestamp=1457101972736, value=beijing
 lisi                                 column=info:age, timestamp=1457101972548, value=27
 lisi                                 column=info:birthday, timestamp=1457101972604, value=1987-06-17
 lisi                                 column=info:company, timestamp=1457101972653, value=baidu
 xiaoming                             column=address:city, timestamp=1457082196082, value=hangzhou
 xiaoming                             column=address:contry, timestamp=1457082195729, value=china
 xiaoming                             column=address:province, timestamp=1457082195773, value=zhejiang
 xiaoming                             column=info:age, timestamp=1457082218735, value=29
 xiaoming                             column=info:age, timestamp=1457082178630, value=24
 xiaoming                             column=info:birthday, timestamp=1457082186830, value=1987-06-17
 xiaoming                             column=info:company, timestamp=1457082189826, value=alibaba
2 row(s) in 0.0260 seconds
```
读取后数据(4列)

	| rowKey  | column:qualifier| timestamp | value |
| --------| ---------------- |-----     |-----  |
| lisi  | address:city| 1457101972764 | beijing |
| lisi  | address:contry| 1457102773908 | china |
| lisi  | address:province| 1457101972736 | beijing |
| lisi  | info:age| 1457101972548 | 27 |
| lisi  | info:birthday| 1457101972604 | 1987-06-17 |
| lisi  | info:company| 1457101972653 | beijing |
| xiaoming  | address:city| 1457082196082 | hangzhou |
| xiaoming  | address:contry| 1457082195729 | china |
| xiaoming  | address:province| 1457082195773 | zhejiang |
| xiaoming  | info:age| 1457082218735 | 29 |
| xiaoming  | info:age| 1457082178630 | 24 |
| xiaoming  | info:birthday| 1457082186830 | 1987-06-17 |
| xiaoming  | info:company| 1457082189826 | alibaba |


3、HbaseReader中有一个必填配置项是：hbaseConfig，需要你联系 HBase PE，将hbase-site.xml 中与连接 HBase 相关的配置项提取出来，以 json 格式填入，同时可以补充更多HBase client的配置，如：设置scan的cache（hbase.client.scanner.caching）、batch来优化与服务器的交互。    


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
              "hbase.rootdir": "hdfs: //ip:9000/hbase",
              "hbase.cluster.distributed": "true",
              "hbase.zookeeper.quorum": "***"
            }
```

### 1.2 限制

1、目前不支持动态列的读取。考虑网络传输流量（支持动态列，需要先将hbase所有列的数据读取出来，再按规则进行过滤），现支持的两种读取模式中需要用户明确指定要读取的列。

2、关于同步作业的切分：目前的切分方式是根据用户hbase表数据的region分布进行切分。即：在用户填写的［startrowkey，endrowkey］范围内，一个region会切分成一个task，单个region不进行切分。 

3、multiVersionFixedColumn模式下不支持增加常量列


## 2 实现原理

简而言之，HbaseReader 通过 HBase 的 Java 客户端，通过 HTable, Scan, ResultScanner 等 API，读取你指定 rowkey 范围内的数据，并将读取的数据使用 DataX 自定义的数据类型拼装为抽象的数据集，并传递给下游 Writer 处理。hbase11xreader与hbase094xreader的主要不同在于API的调用不同，Hbase1.1.x废弃了很多Hbase0.94.x的api。



## 3 功能说明

### 3.1 配置样例

* 配置一个从 HBase 抽取数据到本地的作业:（normal 模式）

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
                    "name": "hbase11xreader",
                    "parameter": {
                        "hbaseConfig": {
                            "hbase.rootdir": "hdfs: //xxx: 9000/hbase",
                            "hbase.cluster.distributed": "true",
                            "hbase.zookeeper.quorum": "xxx"
                        },
                        "table": "users",
                        "encoding": "utf-8",
                        "mode": "normal",
                        "column": [
                            {
                                "name": "rowkey",
                                "type": "string"
                            },
                            {
                                "name": "info: age",
                                "type": "string"
                            },
                            {
                                "name": "info: birthday",
                                "type": "date",
                                "format":"yyyy-MM-dd"
                            },
                            {
                                "name": "info: company",
                                "type": "string"
                            },
                            {
                                "name": "address: contry",
                                "type": "string"
                            },
                            {
                                "name": "address: province",
                                "type": "string"
                            },
                            {
                                "name": "address: city",
                                "type": "string"
                            }
                        ],
                        "range": {
                            "startRowkey": "",
                            "endRowkey": "",
                            "isBinaryRowkey": true
                        }
                    }
                },
                "writer": {
                    "name": "txtfilewriter",
                    "parameter": {
                        "path": "/Users/shf/workplace/datax_test/hbase11xreader/result",
                        "fileName": "qiran",
                        "writeMode": "truncate"
                    }
                }
            }
        ]
    }
}
```

* 配置一个从 HBase 抽取数据到本地的作业:（ multiVersionFixedColumn 模式）

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
          "name": "hbase11xreader",
          "parameter": {
            "hbaseConfig": {
              "hbase.rootdir": "hdfs: //xxx: 9000/hbase",
              "hbase.cluster.distributed": "true",
              "hbase.zookeeper.quorum": "xxx"
            },
            "table": "users",
            "encoding": "utf-8",
            "mode": "multiVersionFixedColumn",
            "maxVersion": "-1",
            "column": [
                {
                    "name": "rowkey",
                    "type": "string"
                },
                {
                    "name": "info: age",
                    "type": "string"
                },
                {
                    "name": "info: birthday",
                    "type": "date",
                    "format":"yyyy-MM-dd"
                },
                {
                    "name": "info: company",
                    "type": "string"
                },
                {
                    "name": "address: contry",
                    "type": "string"
                },
                {
                    "name": "address: province",
                    "type": "string"
                },
                {
                    "name": "address: city",
                    "type": "string"
                }
            ],
            "range": {
              "startRowkey": "",
              "endRowkey": ""
            }
          }
        },
        "writer": {
          "name": "txtfilewriter",
          "parameter": {
            "path": "/Users/shf/workplace/datax_test/hbase11xreader/result",
            "fileName": "qiran",
            "writeMode": "truncate"
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
 
	* 描述：读取hbase的模式，支持normal 模式、multiVersionFixedColumn模式，即：normal/multiVersionFixedColumn <br />
 
	* 必选：是 <br />
 
	* 默认值：无 <br />
	
* **table**
 
	* 描述：要读取的 hbase 表名（大小写敏感） <br />
 
	* 必选：是 <br />
 
	* 默认值：无 <br />

* **encoding**

	* 描述：编码方式，UTF-8 或是 GBK，用于对二进制存储的 HBase byte[] 转为 String 时的编码 <br />
 
	* 必选：否 <br />
 
	* 默认值：UTF-8 <br />
 
  
* **column**

	* 描述：要读取的hbase字段，normal 模式与multiVersionFixedColumn 模式下必填项。    
	(1)、normal 模式下：name指定读取的hbase列，除了rowkey外，必须为 列族:列名 的格式，type指定源数据的类型，format指定日期类型的格式，value指定当前类型为常量，不从hbase读取数据，而是根据value值自动生成对应的列。配置格式如下：
	
	```
	"column": 
[
	{
	    "name": "rowkey",
	    "type": "string"
	},
	{
	    "value": "test",
	    "type": "string"
	}
] 	
		            
	```
    normal 模式下，对于用户指定Column信息，type必须填写，name/value必须选择其一。    
    
    (2)、multiVersionFixedColumn 模式下：name指定读取的hbase列，除了rowkey外，必须为 列族:列名 的格式，type指定源数据的类型，format指定日期类型的格式 。multiVersionFixedColumn模式下不支持常量列。配置格式如下：
		
	```
	"column": 
[
	{
	    "name": "rowkey",
	    "type": "string"
	},
	{
	    "name": "info: age",
	    "type": "string"
	}
]	
	```

	* 必选：是<br />
 
	* 默认值：无 <br />

	
* **maxVersion**

	* 描述：指定在多版本模式下的hbasereader读取的版本数，取值只能为－1或者大于1的数字，－1表示读取所有版本 <br />
	  
	* 必选：multiVersionFixedColumn 模式下必填项<br />
 
	* 默认值：无<br />	
 
* **range**

	* 描述：指定hbasereader读取的rowkey范围。<br />   
	startRowkey：指定开始rowkey；<br />   
	endRowkey指定结束rowkey；<br />  
	isBinaryRowkey：指定配置的startRowkey和endRowkey转换为byte[]时的方式，默认值为false,若为true，则调用Bytes.toBytesBinary(rowkey)方法进行转换;若为false：则调用Bytes.toBytes(rowkey)<br />  
	配置格式如下：   
	
	```
	"range": {
  "startRowkey": "aaa",
  "endRowkey": "ccc",
  "isBinaryRowkey":false
}
	```
	<br />
	  
	* 必选：否 <br />
 
	* 默认值：无 <br />

* **scanCacheSize**

	* 描述：Hbase client每次rpc从服务器端读取的行数 <br />
	  
	* 必选：否<br />
 
	* 默认值：256<br />

* **scanBatchSize**

	* 描述：Hbase client每次rpc从服务器端读取的列数 <br />
	  
	* 必选：否<br />
 
	* 默认值：100<br />


### 3.3 类型转换


下面列出支持的读取HBase数据类型，HbaseReader 针对 HBase 类型转换列表:

| DataX 内部类型| HBase 数据类型    |
| -------- | -----  |
| Long     |int, short ,long|
| Double   |float, double|
| String   |string,binarystring | 
| Date     |date    | 
| Boolean  |boolean  |  




请注意:

* `除上述罗列字段类型外，其他类型均不支持`。

## 4 性能报告

略

## 5 约束限制

略
	

## 6 FAQ

***
