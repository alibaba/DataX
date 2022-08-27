# DataX OBSWriter 说明

------------

针对华为云OBS对象存储的插件，由潍坊雷鸣云网络科技 www.leimingyun.com 开发。

## 1 快速介绍

OBSWriter提供了向OBS写入类CSV格式的一个或者多个表文件。

**写入OBS内容存放的是一张逻辑意义上的二维表，例如CSV格式的文本信息。**


* OBS 产品介绍, 参看[[华为云OBS](https://support.huaweicloud.com/obs/index.html)]
* OBS Java SDK, 参看[[华为云OBS Java SDK](https://support.huaweicloud.com/sdk-java-devg-obs/obs_21_0001.html)]


## 2 功能与限制

OBSWriter实现了从DataX协议转为OBS中的TXT文件功能，OBS本身是无结构化数据存储，OBSWriter需要在如下几个方面增加:

1. 支持且仅支持写入 TXT的文件，且要求TXT中shema为一张二维表。

2. 支持类CSV格式文件，自定义分隔符。

3. 暂时不支持文本压缩。

6. 支持多线程写入，每个线程写入不同子文件。

7. 文件支持滚动，当文件大于某个size值或者行数值，文件需要切换。 [暂不支持]

我们不能做到：

1. 单个文件不能支持并发写入。


## 3 功能说明


### 3.1 配置样例

```json
{
    "job": {
        "setting": {},
        "content": [
            {
                "reader": {

                },
                "writer": {
					"name": "obswriter",
					"parameter": {
						  "endpoint": "https://obs.cn-north-4.myhuaweicloud.com",
						  "accessKey": "yourAccessKey",
						  "secretKey": "yourSecretKey",
						  "bucket": "yourBucket",
						  "object": "obstest/datax",
						  "writeMode": "truncate|append|nonConflict",
						  "fieldDelimiter": ",",
						  "encoding": "UTF-8"
                    }
				}
            }
        ]
    }
}
```

### 3.2 参数说明

* **endpoint**

	* 描述：OBS Server的EndPoint地址，例如https://obs.cn-north-4.myhuaweicloud.com。

	* 必选：是 <br />

	* 默认值：无 <br />

* **accessKey**

	* 描述：OBS的accessKey <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **secretKey**

	* 描述：OBS的secretKey  <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **bucket**

	* 描述：OBS的bucket  <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **object**

 	* 描述：OBSWriter写入的文件名，OBS使用文件名模拟目录的实现。 <br />

		使用"object": "datax"，写入object以datax开头，后缀添加随机字符串。
		使用"object": "obstest/datax"，写入的object以obstest/datax开头，后缀随机添加字符串，/作为OBS模拟目录的分隔符。

	* 必选：是 <br />

	* 默认值：无 <br />

* **writeMode**

 	* 描述：OBSWriter写入前数据清理处理： <br />

		* truncate，写入前清理object名称前缀匹配的所有object。例如: "object": "abc"，将清理所有abc开头的object。
		* append，写入前不做任何处理，DataX OBSWriter直接使用object名称写入，并使用随机UUID的后缀名来保证文件名不冲突。例如用户指定的object名为datax，实际写入为datax_xxxxxx_xxxx_xxxx
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


* **encrypt**

	* 描述：是否在服务器端进行加密存储。<br />

		* false，不加密存储
		* true，加密存储

 	* 必选：否 <br />

 	* 默认值：false <br />



### 3.3 类型转换

## 4 性能报告

OBS本身不提供数据类型，该类型是DataX OBSWriter定义：

| DataX 内部类型| OBS 数据类型    |
| -------- | -----  |
| Long     |Long |
| Double   |Double|
| String   |String|
| Boolean  |Boolean |
| Date     |Date |

其中：

* OBS Long是指OBS文本中使用整形的字符串表示形式，例如"19901219"。
* OBS Double是指OBS文本中使用Double的字符串表示形式，例如"3.1415"。
* OBS Boolean是指OBS文本中使用Boolean的字符串表示形式，例如"true"、"false"。不区分大小写。
* OBS Date是指OBS文本中使用Date的字符串表示形式，例如"2014-12-31"，Date可以指定format格式。


## 5 约束限制

略

## 6 FAQ

略



