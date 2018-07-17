# DataX JsonFileReader 说明


------------

## 1 快速介绍

JsonFileReader提供了读取本地文件系统数据存储的能力。在底层实现上，JsonFileReader获取本地文件数据，使用Jayway JsonPath抽取Json字符串，并转换为DataX传输协议传递给Writer。



## 2 功能与限制

JsonFileReader实现了从本地文件读取数据并转为DataX协议的功能，本地文件是可以是Json数据格式的集合，对于DataX而言，JsonFileReader实现上类比TxtFileReader，有诸多相似之处。目前JsonFileReader支持功能如下：

1. 支持且仅支持读取TXT的文件，且要求TXT中s内容必须符合json。

2. 支持列常量和Json的Key为空值

4. 支持递归读取、支持文件名过滤。

6. 多个File可以支持并发读取。

我们暂时不能做到：

1. 单个File支持多线程并发读取，这里涉及到单个File内部切分算法。

2.  单个File在压缩情况下，从技术上无法支持多线程并发读取。

3.  暂不支持读取压缩文件和日期类型的自定义 日期


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
                name": "jsonfilereader",
                "parameter": {
                              "path": [
                                       "/home/unicom/"
                                       ],
                              "column": [
                                        {
                                                "index": "$.a",
                                                "type": "long"
                                              },
                                              {
                                                "index": "$.b",
                                                "type": "boolean"
                                              },
                                              {
                                                "index": "$.c.d",
                                                "type": "double"
                                              },
                                              {
                                                "index": "$.e[0]",
                                                "type": "string"
                                              },
                                              {
                                                "index": "$.f",
                                                "type": "date"
                                              }
                                          ]
                                               }
                },
                "writer": {
                           "name": "streamwriter",
                           "parameter": {
                            "encoding": "",
                            "print": true
                            }
                }
        ]
    }
}
```

### 3.2 参数说明

* **path**

	* 描述：本地文件系统的路径信息，注意这里可以支持填写多个路径。 <br />

		 当指定单个本地文件，JsonFileReader暂时只能使用单线程进行数据抽取。

		当指定多个本地文件，JsonFileReader支持使用多线程进行数据抽取。线程并发数通过通道数指定。

		当指定通配符，JsonFileReader尝试遍历出多个文件信息。例如: 指定/*代表读取/目录下所有的文件，指定/bazhen/\*代表读取bazhen目录下游所有的文件。**JsonFileReader目前只支持\*作为文件通配符。**

		**特别需要注意的是，如果Path指定的路径下没有符合匹配的文件抽取，DataX将报错。**

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：读取字段列表，type指定源数据的类型，index指定当前列来自于json的指定，语法为Jayway JsonPath的语法，value指定当前类型为常量，不从源头文件读取数据，而是根据value值自动生成对应的列。 <br />

		用户必须指定Column字段信息，配置如下：

		```json
		{
           "index": "$.a",
           "type": "long"
         },
         {
         "index": "$.c.d",
         "type": "double"
         },
         {
          "index": "$.e[0]",
          "type": "string"
          }
		```

		对于用户指定Column信息，type必须填写，index/value必须选择其一。上面表示从下面的json:
		```json
		{
        	"a": 1,
        	"b": 2,
        	"c": {
        		"d": 4
        	},
        	"e": [5]
        }
        ```
        抽取的数据1, 4, 5形成类似二维表的形式，如果数据中对应的Key值没有的话，会自动补充上null值。

	* 必选：是 <br />

	* 默认值：无 <br />

* **encoding**

	* 描述：读取文件的编码配置。<br />

 	* 必选：否 <br />

 	* 默认值：utf-8 <br />


### 3.3 类型转换

本地文件本身不提供数据类型，该类型是DataX JsonFileReade定义：

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
* 本地文件 Date是指本地文件文本中使用Date的字符串表示形式，例如"2014-12-31"，Date不可以指定format格式。


## 4 性能报告



## 5 约束限制

略

## 6 FAQ

略


