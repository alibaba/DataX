# DataX HdfsReader 插件文档


------------

## 1 快速介绍

HdfsReader提供了读取分布式文件系统数据存储的能力。在底层实现上，HdfsReader获取分布式文件系统上文件的数据，并转换为DataX传输协议传递给Writer。

**目前HdfsReader支持的文件格式有textfile（text）、orcfile（orc）、rcfile（rc）、sequence file（seq）和普通逻辑二维表（csv）类型格式的文件，且文件内容存放的必须是一张逻辑意义上的二维表。**

**HdfsReader需要Jdk1.7及以上版本的支持。**


## 2 功能与限制

HdfsReader实现了从Hadoop分布式文件系统Hdfs中读取文件数据并转为DataX协议的功能。textfile是Hive建表时默认使用的存储格式，数据不做压缩，本质上textfile就是以文本的形式将数据存放在hdfs中，对于DataX而言，HdfsReader实现上类比TxtFileReader，有诸多相似之处。orcfile，它的全名是Optimized Row Columnar file，是对RCFile做了优化。据官方文档介绍，这种文件格式可以提供一种高效的方法来存储Hive数据。HdfsReader利用Hive提供的OrcSerde类，读取解析orcfile文件的数据。目前HdfsReader支持的功能如下：

1. 支持textfile、orcfile、rcfile、sequence file和csv格式的文件，且要求文件内容存放的是一张逻辑意义上的二维表。

2. 支持多种类型数据读取(使用String表示)，支持列裁剪，支持列常量

3. 支持递归读取、支持正则表达式（"*"和"?"）。

4. 支持orcfile数据压缩，目前支持SNAPPY，ZLIB两种压缩方式。

5. 多个File可以支持并发读取。

6. 支持sequence file数据压缩，目前支持lzo压缩方式。

7. csv类型支持压缩格式有：gzip、bz2、zip、lzo、lzo_deflate、snappy。

8. 目前插件中Hive版本为1.1.1，Hadoop版本为2.7.1（Apache［为适配JDK1.7］,在Hadoop 2.5.0, Hadoop 2.6.0 和Hive 1.2.0测试环境中写入正常；其它版本需后期进一步测试； 

9. 支持kerberos认证（注意：如果用户需要进行kerberos认证，那么用户使用的Hadoop集群版本需要和hdfsreader的Hadoop版本保持一致，如果高于hdfsreader的Hadoop版本，不保证kerberos认证有效）

我们暂时不能做到：

1. 单个File支持多线程并发读取，这里涉及到单个File内部切分算法。二期考虑支持。
2. 目前还不支持hdfs HA;



## 3 功能说明


### 3.1 配置样例

```json
{
    "job": {
        "setting": {
            "speed": {
                "channel": 3
            }
        },
        "content": [
            {
                "reader": {
                    "name": "hdfsreader",
                    "parameter": {
                        "path": "/user/hive/warehouse/mytable01/*",
                        "defaultFS": "hdfs://xxx:port",
                        "column": [
                               {
                                "index": 0,
                                "type": "long"
                               },
                               {
                                "index": 1,
                                "type": "boolean"
                               },
                               {
                                "type": "string",
                                "value": "hello"
                               },
                               {
                                "index": 2,
                                "type": "double"
                               }
                        ],
                        "fileType": "orc",
                        "encoding": "UTF-8",
                        "fieldDelimiter": ","
                    }

                },
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print": true
                    }
                }
            }
        ]
    }
}
```

### 3.2 参数说明（各个配置项值前后不允许有空格）

* **path**

	* 描述：要读取的文件路径，如果要读取多个文件，可以使用正则表达式"*"，注意这里可以支持填写多个路径。。 <br />

		当指定单个Hdfs文件，HdfsReader暂时只能使用单线程进行数据抽取。二期考虑在非压缩文件情况下针对单个File可以进行多线程并发读取。

		当指定多个Hdfs文件，HdfsReader支持使用多线程进行数据抽取。线程并发数通过通道数指定。

		当指定通配符，HdfsReader尝试遍历出多个文件信息。例如: 指定/*代表读取/目录下所有的文件，指定/bazhen/\*代表读取bazhen目录下游所有的文件。HdfsReader目前只支持"*"和"?"作为文件通配符。

		**特别需要注意的是，DataX会将一个作业下同步的所有的文件视作同一张数据表。用户必须自己保证所有的File能够适配同一套schema信息。并且提供给DataX权限可读。**


	* 必选：是 <br />

	* 默认值：无 <br />

* **defaultFS**

	* 描述：Hadoop hdfs文件系统namenode节点地址。 <br />


		**目前HdfsReader已经支持Kerberos认证，如果需要权限认证，则需要用户配置kerberos参数，见下面**


	* 必选：是 <br />

	* 默认值：无 <br />

* **fileType**

	* 描述：文件的类型，目前只支持用户配置为"text"、"orc"、"rc"、"seq"、"csv"。 <br />

		text表示textfile文件格式

		orc表示orcfile文件格式
		
		rc表示rcfile文件格式
		
		seq表示sequence file文件格式
		
		csv表示普通hdfs文件格式（逻辑二维表）

		**特别需要注意的是，HdfsReader能够自动识别文件是orcfile、textfile或者还是其它类型的文件，但该项是必填项，HdfsReader则会只读取用户配置的类型的文件，忽略路径下其他格式的文件**

		**另外需要注意的是，由于textfile和orcfile是两种完全不同的文件格式，所以HdfsReader对这两种文件的解析方式也存在差异，这种差异导致hive支持的复杂复合类型(比如map,array,struct,union)在转换为DataX支持的String类型时，转换的结果格式略有差异，比如以map类型为例：**

		orcfile map类型经hdfsreader解析转换成datax支持的string类型后，结果为"{job=80, team=60, person=70}"

		textfile map类型经hdfsreader解析转换成datax支持的string类型后，结果为"job:80,team:60,person:70"

		从上面的转换结果可以看出，数据本身没有变化，但是表示的格式略有差异，所以如果用户配置的文件路径中要同步的字段在Hive中是复合类型的话，建议配置统一的文件格式。

		**如果需要统一复合类型解析出来的格式，我们建议用户在hive客户端将textfile格式的表导成orcfile格式的表**

	* 必选：是 <br />

	* 默认值：无 <br />


* **column**

	* 描述：读取字段列表，type指定源数据的类型，index指定当前列来自于文本第几列(以0开始)，value指定当前类型为常量，不从源头文件读取数据，而是根据value值自动生成对应的列。 <br />

		默认情况下，用户可以全部按照String类型读取数据，配置如下：

		```json
		"column": ["*"]
		```

		用户可以指定Column字段信息，配置如下：

		```json
		{
		  "type": "long",
		  "index": 0    //从本地文件文本第一列获取int字段
		},
		{
		  "type": "string",
		  "value": "alibaba"  //HdfsReader内部生成alibaba的字符串字段作为当前字段
		}
		```

		对于用户指定Column信息，type必须填写，index/value必须选择其一。

	* 必选：是 <br />

	* 默认值：全部按照string类型读取 <br />

* **fieldDelimiter**

	* 描述：读取的字段分隔符 <br />

	**另外需要注意的是，HdfsReader在读取textfile数据时，需要指定字段分割符，如果不指定默认为','，HdfsReader在读取orcfile时，用户无需指定字段分割符**

	* 必选：否 <br />

	* 默认值：, <br />


* **encoding**

	* 描述：读取文件的编码配置。<br />

 	* 必选：否 <br />

 	* 默认值：utf-8 <br />


* **nullFormat**

	* 描述：文本文件中无法使用标准字符串定义null(空指针)，DataX提供nullFormat定义哪些字符串可以表示为null。<br />

		 例如如果用户配置: nullFormat:"\\N"，那么如果源头数据是"\N"，DataX视作null字段。

 	* 必选：否 <br />

 	* 默认值：无 <br />

* **haveKerberos**

	* 描述：是否有Kerberos认证，默认false<br />
 
		 例如如果用户配置true，则配置项kerberosKeytabFilePath，kerberosPrincipal为必填。

 	* 必选：haveKerberos 为true必选 <br />
 
 	* 默认值：false <br />

* **kerberosKeytabFilePath**

	* 描述：Kerberos认证 keytab文件路径，绝对路径<br />

 	* 必选：否 <br />
 
 	* 默认值：无 <br />

* **kerberosPrincipal**

	* 描述：Kerberos认证Principal名，如xxxx/hadoopclient@xxx.xxx <br />

 	* 必选：haveKerberos 为true必选 <br />
 
 	* 默认值：无 <br />

* **compress**

	* 描述：当fileType（文件类型）为csv下的文件压缩方式，目前仅支持 gzip、bz2、zip、lzo、lzo_deflate、hadoop-snappy、framing-snappy压缩；**值得注意的是，lzo存在两种压缩格式：lzo和lzo_deflate，用户在配置的时候需要留心，不要配错了；另外，由于snappy目前没有统一的stream format，datax目前只支持最主流的两种：hadoop-snappy（hadoop上的snappy stream format）和framing-snappy（google建议的snappy stream format）**;orc文件类型下无需填写。<br />

 	* 必选：否 <br />
 
 	* 默认值：无 <br />
	
* **hadoopConfig**

	* 描述：hadoopConfig里可以配置与Hadoop相关的一些高级参数，比如HA的配置。<br />

		```json
		"hadoopConfig":{
		        "dfs.nameservices": "testDfs",
		        "dfs.ha.namenodes.testDfs": "namenode1,namenode2",
		        "dfs.namenode.rpc-address.aliDfs.namenode1": "",
		        "dfs.namenode.rpc-address.aliDfs.namenode2": "",
		        "dfs.client.failover.proxy.provider.testDfs": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
		}
		```

	* 必选：否 <br />
 
 	* 默认值：无 <br />

* **csvReaderConfig**

	* 描述：读取CSV类型文件参数配置，Map类型。读取CSV类型文件使用的CsvReader进行读取，会有很多配置，不配置则使用默认值。<br />

 	* 必选：否 <br />
 
 	* 默认值：无 <br />

        
常见配置：

```json
"csvReaderConfig":{
        "safetySwitch": false,
        "skipEmptyRecords": false,
        "useTextQualifier": false
}
```

所有配置项及默认值,配置时 csvReaderConfig 的map中请**严格按照以下字段名字进行配置**：

```
boolean caseSensitive = true;
char textQualifier = 34;
boolean trimWhitespace = true;
boolean useTextQualifier = true;//是否使用csv转义字符
char delimiter = 44;//分隔符
char recordDelimiter = 0;
char comment = 35;
boolean useComments = false;
int escapeMode = 1;
boolean safetySwitch = true;//单列长度是否限制100000字符
boolean skipEmptyRecords = true;//是否跳过空行
boolean captureRawRecord = true;
```

### 3.3 类型转换

由于textfile和orcfile文件表的元数据信息由Hive维护并存放在Hive自己维护的数据库（如mysql）中，目前HdfsReader不支持对Hive元数

据数据库进行访问查询，因此用户在进行类型转换的时候，必须指定数据类型，如果用户配置的column为"*"，则所有column默认转换为

string类型。HdfsReader提供了类型转换的建议表如下：

| DataX 内部类型| Hive表 数据类型    |
| -------- | -----  |
| Long     |TINYINT,SMALLINT,INT,BIGINT|
| Double   |FLOAT,DOUBLE|
| String   |String,CHAR,VARCHAR,STRUCT,MAP,ARRAY,UNION,BINARY|
| Boolean  |BOOLEAN|
| Date     |Date,TIMESTAMP|

其中：

* Long是指Hdfs文件文本中使用整形的字符串表示形式，例如"123456789"。
* Double是指Hdfs文件文本中使用Double的字符串表示形式，例如"3.1415"。
* Boolean是指Hdfs文件文本中使用Boolean的字符串表示形式，例如"true"、"false"。不区分大小写。
* Date是指Hdfs文件文本中使用Date的字符串表示形式，例如"2014-12-31"。

特别提醒：

* Hive支持的数据类型TIMESTAMP可以精确到纳秒级别，所以textfile、orcfile中TIMESTAMP存放的数据类似于"2015-08-21 22:40:47.397898389"，如果转换的类型配置为DataX的Date，转换之后会导致纳秒部分丢失，所以如果需要保留纳秒部分的数据，请配置转换类型为DataX的String类型。


### 3.4 按分区读取

Hive在建表的时候，可以指定分区partition，例如创建分区partition(day="20150820",hour="09")，对应的hdfs文件系统中，相应的表的目录下则会多出/20150820和/09两个目录，且/20150820是/09的父目录。了解了分区都会列成相应的目录结构，在按照某个分区读取某个表所有数据时，则只需配置好json中path的值即可。

比如需要读取表名叫mytable01下分区day为20150820这一天的所有数据，则配置如下：

```json
"path": "/user/hive/warehouse/mytable01/20150820/*"
```


## 4 性能报告



## 5 约束限制

略

## 6 FAQ

1. 如果报java.io.IOException: Maximum column length of 100,000 exceeded in column...异常信息，说明数据源column字段长度超过了100000字符。

 需要在json的reader里增加如下配置
 ```json
 "csvReaderConfig":{
	"safetySwitch": false,
	"skipEmptyRecords": false,
	"useTextQualifier": false
 }
 ```
 safetySwitch = false;//单列长度不限制100000字符

