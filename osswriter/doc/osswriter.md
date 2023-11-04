# DataX OSSWriter 说明


------------

## 1 快速介绍

OSSWriter提供了向OSS写入类CSV格式的一个或者多个表文件。

**写入OSS内容存放的是一张逻辑意义上的二维表，例如CSV格式的文本信息。**


* OSS 产品介绍, 参看[[阿里云OSS Portal](http://www.aliyun.com/product/oss)]
* OSS Java SDK, 参看[[阿里云OSS Java SDK](http://oss.aliyuncs.com/aliyun_portal_storage/help/oss/OSS_Java_SDK_Dev_Guide_20141113.pdf)]


## 2 功能与限制

OSSWriter实现了从DataX协议转为OSS中的TXT文件功能，OSS本身是无结构化数据存储，OSSWriter需要在如下几个方面增加:

1. 支持写入 TXT的文件，且要求TXT中shema为一张二维表。

2. 支持类CSV格式文件，自定义分隔符。

3. 暂时不支持文本压缩。

6. 支持多线程写入，每个线程写入不同子文件。

7. 文件支持滚动，当文件大于某个size值或者行数值，文件需要切换。 [暂不支持]

8. 支持写 PARQUET、ORC 文件

我们不能做到：

1. 单个文件不能支持并发写入。


## 3 功能说明


### 3.1 配置样例
写 txt文件样例
```json
{
    "job": {
        "setting": {},
        "content": [
            {
                "reader": {

                },
                "writer": {
                      "name": "osswriter",
                      "parameter": {
                        "endpoint": "http://oss.aliyuncs.com",
                        "accessId": "",
                        "accessKey": "",
                        "bucket": "myBucket",
                        "object": "cdo/datax",
                        "encoding": "UTF-8",
                        "fieldDelimiter": ",",
                        "writeMode": "truncate|append|nonConflict"
                    }
				}
            }
        ]
    }
}
```
写 orc 文件样例
```json
{
	"job": {
		"setting": {},
		"content": [
			{
				"reader": {},
				"writer": {
					"name": "osswriter",
					"parameter": {
						"endpoint": "http://oss.aliyuncs.com",
						"accessId": "",
						"accessKey": "",
						"bucket": "myBucket",
						"fileName": "test",
						"encoding": "UTF-8",
						"column": [
							{
								"name": "col1",
								"type": "BIGINT"
							},
							{
								"name": "col2",
								"type": "DOUBLE"
							},
							{
								"name": "col3",
								"type": "STRING"
							}
						],
						"fileFormat": "orc",
						"path": "/tests/case61",
						"writeMode": "append"
					}
				}
			}
		]
	}
}
```

写 parquet 文件样例
```json
{
	"job": {
		"setting": {},
		"content": [
			{
				"reader": {},
				"writer": {
					"name": "osswriter",
					"parameter": {
						"endpoint": "http://oss.aliyuncs.com",
						"accessId": "",
						"accessKey": "",
						"bucket": "myBucket",
						"fileName": "test",
						"encoding": "UTF-8",
						"column": [
							{
								"name": "col1",
								"type": "BIGINT"
							},
							{
								"name": "col2",
								"type": "DOUBLE"
							},
							{
								"name": "col3",
								"type": "STRING"
							}
						],
						"parquetSchema": "message test { required int64 int64_col;\n required binary str_col (UTF8);\nrequired group params (MAP) {\nrepeated group key_value {\nrequired binary key (UTF8);\nrequired binary value (UTF8);\n}\n}\nrequired group params_arr (LIST) {\n  repeated group list {\n    required binary element (UTF8);\n  }\n}\nrequired group params_struct {\n  required int64 id;\n required binary name (UTF8);\n }\nrequired group params_arr_complex (LIST) {\n  repeated group list {\n    required group element {\n required int64 id;\n required binary name (UTF8);\n}\n  }\n}\nrequired group params_complex (MAP) {\nrepeated group key_value {\nrequired binary key (UTF8);\nrequired group value {\n  required int64 id;\n required binary name (UTF8);\n  }\n}\n}\nrequired group params_struct_complex {\n  required int64 id;\n required group detail {\n  required int64 id;\n required binary name (UTF8);\n  }\n  }\n}",
						"fileFormat": "parquet",
						"path": "/tests/case61",
						"writeMode": "append"
					}
				}
			}
		]
	}
}
```
### 3.2 参数说明

* **endpoint**

	* 描述：OSS Server的EndPoint地址，例如http://oss.aliyuncs.com。

	* 必选：是 <br />

	* 默认值：无 <br />

* **accessId**

	* 描述：OSS的accessId <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **accessKey**

	* 描述：OSS的accessKey  <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **bucket**

	* 描述：OSS的bucket  <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **object**

 	* 描述：OSSWriter写入的文件名，OSS使用文件名模拟目录的实现。 <br />

		使用"object": "datax"，写入object以datax开头，后缀添加随机字符串。
		使用"object": "cdo/datax"，写入的object以cdo/datax开头，后缀随机添加字符串，/作为OSS模拟目录的分隔符。

	* 必选：是 <br />

	* 默认值：无 <br />

* **writeMode**

 	* 描述：OSSWriter写入前数据清理处理： <br />

		* truncate，写入前清理object名称前缀匹配的所有object。例如: "object": "abc"，将清理所有abc开头的object。
		* append，写入前不做任何处理，DataX OSSWriter直接使用object名称写入，并使用随机UUID的后缀名来保证文件名不冲突。例如用户指定的object名为datax，实际写入为datax_xxxxxx_xxxx_xxxx
		* nonConflict，如果指定路径出现前缀匹配的object，直接报错。例如: "object": "abc"，如果存在abc123的object，将直接报错。

	* 必选：是 <br />

	* 默认值：无 <br />

* **fieldDelimiter**

	* 描述：读取的字段分隔符 <br />

	* 必选：否 <br />

	* 默认值：, <br />


* **encoding**

	* 描述：写出文件的编码配置。<br />

 	* 必选：否 <br />

 	* 默认值：utf-8 <br />


* **nullFormat**

	* 描述：文本文件中无法使用标准字符串定义null(空指针)，DataX提供nullFormat定义哪些字符串可以表示为null。<br />

		 例如如果用户配置: nullFormat="\N"，那么如果源头数据是"\N"，DataX视作null字段。

 	* 必选：否 <br />

 	* 默认值：\N <br />
* **dateFormat**

	* 描述：日期类型的数据序列化到object中时的格式，例如 "dateFormat": "yyyy-MM-dd"。<br />


 	* 必选：否 <br />

 	* 默认值：无 <br />

* **fileFormat**

	* 描述：文件写出的格式，包括csv  (http://zh.wikipedia.org/wiki/%E9%80%97%E5%8F%B7%E5%88%86%E9%9A%94%E5%80%BC) 和text两种，csv是严格的csv格式，如果待写数据包括列分隔符，则会按照csv的转义语法转义，转义符号为双引号"；text格式是用列分隔符简单分割待写数据，对于待写数据包括列分隔符情况下不做转义。<br />

 	* 必选：否 <br />

 	* 默认值：text <br />

* **header**

	* 描述：Oss写出时的表头，示例['id', 'name', 'age']。<br />

 	* 必选：否 <br />

 	* 默认值：无 <br />

* **maxFileSize**

	* 描述：Oss写出时单个Object文件的最大大小，默认为10000*10MB，类似log4j日志打印时根据日志文件大小轮转。OSS分块上传时，每个分块大小为10MB，每个OSS InitiateMultipartUploadRequest支持的分块最大数量为10000。轮转发生时，object名字规则是：在原有object前缀加UUID随机数的基础上，拼接_1,_2,_3等后缀。<br />

 	* 必选：否 <br />

 	* 默认值：100000MB <br />

### 3.3 类型转换

## 4 性能报告

OSS本身不提供数据类型，该类型是DataX OSSWriter定义：

| DataX 内部类型| OSS 数据类型    |
| -------- | -----  |
| Long     |Long |
| Double   |Double|
| String   |String|
| Boolean  |Boolean |
| Date     |Date |

其中：

* OSS Long是指OSS文本中使用整形的字符串表示形式，例如"19901219"。
* OSS Double是指OSS文本中使用Double的字符串表示形式，例如"3.1415"。
* OSS Boolean是指OSS文本中使用Boolean的字符串表示形式，例如"true"、"false"。不区分大小写。
* OSS Date是指OSS文本中使用Date的字符串表示形式，例如"2014-12-31"，Date可以指定format格式。


## 5 约束限制

略

## 6 FAQ

略

