# DataX TxtFileWriter 说明


------------

## 1 快速介绍

TxtFileWriter提供了向本地文件写入类CSV格式的一个或者多个表文件。TxtFileWriter服务的用户主要在于DataX开发、测试同学。

**写入本地文件内容存放的是一张逻辑意义上的二维表，例如CSV格式的文本信息。**


## 2 功能与限制

TxtFileWriter实现了从DataX协议转为本地TXT文件功能，本地文件本身是无结构化数据存储，TxtFileWriter如下几个方面约定:

1. 支持且仅支持写入 TXT的文件，且要求TXT中shema为一张二维表。

2. 支持类CSV格式文件，自定义分隔符。

3. 支持文本压缩，现有压缩格式为gzip、bzip2。

6. 支持多线程写入，每个线程写入不同子文件。

7. 文件支持滚动，当文件大于某个size值或者行数值，文件需要切换。 [暂不支持]

我们不能做到：

1. 单个文件不能支持并发写入。


## 3 功能说明


### 3.1 配置样例

```json
{
    "setting": {},
    "job": {
        "setting": {
            "speed": {
                "channel": 2
            }
        },
        "content": [
            {
                "reader": {
                    "name": "txtfilereader",
                    "parameter": {
                        "path": ["/home/haiwei.luo/case00/data"],
                        "encoding": "UTF-8",
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
                                "index": 2,
                                "type": "double"
                            },
                            {
                                "index": 3,
                                "type": "string"
                            },
                            {
                                "index": 4,
                                "type": "date",
                                "format": "yyyy.MM.dd"
                            }
                        ],
                        "fieldDelimiter": ","
                    }
                },
                "writer": {
                    "name": "txtfilewriter",
                    "parameter": {
                        "path": "/home/haiwei.luo/case00/result",
                        "fileName": "luohw",
                        "writeMode": "truncate",
                        "dateFormat": "yyyy-MM-dd"
                    }
                }
            }
        ]
    }
}
```

### 3.2 参数说明

* **path**

	* 描述：本地文件系统的路径信息，TxtFileWriter会写入Path目录下属多个文件。 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **fileName**

 	* 描述：TxtFileWriter写入的文件名，该文件名会添加随机的后缀作为每个线程写入实际文件名。 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **writeMode**

 	* 描述：TxtFileWriter写入前数据清理处理模式： <br />

		* truncate，写入前清理目录下一fileName前缀的所有文件。
		* append，写入前不做任何处理，DataX TxtFileWriter直接使用filename写入，并保证文件名不冲突。
		* nonConflict，如果目录下有fileName前缀的文件，直接报错。

	* 必选：是 <br />

	* 默认值：无 <br />

* **fieldDelimiter**

	* 描述：读取的字段分隔符 <br />

	* 必选：否 <br />

	* 默认值：, <br />

* **compress**

	* 描述：文本压缩类型，默认不填写意味着没有压缩。支持压缩类型为zip、lzo、lzop、tgz、bzip2。 <br />

	* 必选：否 <br />

	* 默认值：无压缩 <br />

* **encoding**

	* 描述：读取文件的编码配置。<br />

 	* 必选：否 <br />

 	* 默认值：utf-8 <br />


* **nullFormat**

	* 描述：文本文件中无法使用标准字符串定义null(空指针)，DataX提供nullFormat定义哪些字符串可以表示为null。<br />

		 例如如果用户配置: nullFormat="\N"，那么如果源头数据是"\N"，DataX视作null字段。

 	* 必选：否 <br />

 	* 默认值：\N <br />

* **dateFormat**

	* 描述：日期类型的数据序列化到文件中时的格式，例如 "dateFormat": "yyyy-MM-dd"。<br />

 	* 必选：否 <br />

 	* 默认值：无 <br />

* **fileFormat**

	* 描述：文件写出的格式，包括csv (http://zh.wikipedia.org/wiki/%E9%80%97%E5%8F%B7%E5%88%86%E9%9A%94%E5%80%BC) 和text两种，csv是严格的csv格式，如果待写数据包括列分隔符，则会按照csv的转义语法转义，转义符号为双引号"；text格式是用列分隔符简单分割待写数据，对于待写数据包括列分隔符情况下不做转义。<br />

 	* 必选：否 <br />

 	* 默认值：text <br />

* **header**

	* 描述：txt写出时的表头，示例['id', 'name', 'age']。<br />

 	* 必选：否 <br />

 	* 默认值：无 <br />

### 3.3 类型转换


本地文件本身不提供数据类型，该类型是DataX TxtFileWriter定义：

| DataX 内部类型| 本地文件 数据类型    |
| -------- | -----  |
|
| Long     |Long |
| Double   |Double|
| String   |String|
| Boolean  |Boolean |
| Date     |Date |

其中：

* 本地文件 Long是指本地文件文本中使用整形的字符串表示形式，例如"19901219"。
* 本地文件 Double是指本地文件文本中使用Double的字符串表示形式，例如"3.1415"。
* 本地文件 Boolean是指本地文件文本中使用Boolean的字符串表示形式，例如"true"、"false"。不区分大小写。
* 本地文件 Date是指本地文件文本中使用Date的字符串表示形式，例如"2014-12-31"，Date可以指定format格式。


## 4 性能报告


## 5 约束限制

略

## 6 FAQ

略


