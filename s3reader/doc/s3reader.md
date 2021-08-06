# DataX S3Reader 说明

------------

## 1 功能与限制

S3Reader实现了从s3读取数据并转为DataX协议的功能，S3本身是无结构化数据存储，对于DataX而言，S3Reader实现上类比TxtFileReader，有诸多相似之处。目前S3Reader支持功能如下：

1. 支持且仅支持读取TEXTFILE|PARQUET|ORC|AVRO的文件，且要求shema为一张二维表。

2. 支持类CSV格式文件，自定义分隔符。

3. 支持多种数据格式SNAPPY/LZ4/LZO/DEFLATE/XZ,同时支持常见压缩NONE/GZIP/BZIP/BZIP2等等,每种数据格式支持压缩略有不同。

4. jdbc中password和S3中accessKey/secretKey均采用密文形式.(实现见com/alibaba/datax/common/util/Securet)。

5. 多个object可以支持并发读取[暂不支持]。

暂时不能做到：

1. 单个Object(File)支持多线程并发读取，这里涉及到单个Object内部切分算法。后面考虑支持。

2. 单个Object在压缩情况下，从技术上无法支持多线程并发读取。


## 2 功能说明


### 2.1 配置样例

```json
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
					"name": "s3reader",
					"parameter": {
						"accessKey": "******",
						"secretKey": "******",
						"bucket": "******",
						"path": ["tmp/orcfile_snappy/pt=202106510/"],
						"stored": "orc",
						"fieldDelimiter": "\u0001",
						"compression": "snappy",
						"column": [
							{
								"index": 0,
								"type": "bigint"
							},
							{
								"index": 1,
								"type": "bigint"
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
								"type": "decimal(10,2)"
							},
							{
								"index": 5,
								"type": "decimal(10,4)"
							},
							{
								"index": 6,
								"type": "string"
							},
							{
								"index": 7,
								"type": "timestamp"
							},
							{
								"index": 8,
								"type": "date"
							},
							{
								"index": 9,
								"type": "boolean"
							}
						]
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

### 2.2 参数说明

* **accessKey**

	* 描述：S3的accessKey(密文)<br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **secretKey**

	* 描述：S3的secretKey(密文)<br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **bucket**

	* 描述：S3的bucket  <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **path**

	* 描述：S3的object信息，注意这里可以支持填写多个Object。 <br />

	    当指定单个S3 Object，S3Reader暂时只能使用单线程进行数据抽取。

		当指定多个S3 Object，S3Reader支持使用多线程进行数据抽取。线程并发数通过通道数指定[后面支持]。

		**特别需要注意的是，DataX会将一个作业下同步的所有Object视作同一张数据表。用户必须自己保证所有的Object能够适配同一套schema信息。**

	* 必选：是 <br />

	* 默认值：无 <br />
	
    * 建议: S3Writer写入的文件名，S3使用文件名模拟目录的实现;文件和目录格式: 库名/表名/分区/文件名.格式。

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
           "index": 0    //从S3文本第一列获取int字段
        },
        {
           "type": "string",
           "value": "alibaba"  //从S3Reader内部生成alibaba的字符串字段作为当前字段
        }
		```

		对于用户指定Column信息，type必须填写，index/value必须选择其一。

	* 必选：是 <br />

	* 默认值：全部按照string类型读取 <br />

* **stored**

	* 描述：S3写出时支持TEXTFILE|PARQUET|ORC数据格式。<br />

	* 必选：是 <br />

	* 默认值：TEXTFILE <br />

* **compression**

	* 描述：S3写出时支持SNAPPY|GZIP|LZO等等数据压缩。<br />

	  | 数据格式   | 压缩格式                      |
	  | -------- | --------------------         |
	  | TEXT     |NONE/GZIP/BZIP2/SNAPPY/LZ4    |
	  | PARQUET  |NONE/SNAPPY/GZIP/LZO/LZ4      |
	  | ORC      |NONE/SNAPPY/GZIP/BZIP/LZ4     |
	  | AVRO     |NONE/SNAPPY/BZIP2/DEFLATE/XZ  |

	* 必选：是 <br />

	* 默认值：NONE <br />

* **fieldDelimiter**

	* 描述：读取的字段分隔符 <br />

	* 必选：否 <br />

	* 默认值：\u0001 <br />

### 2.3 类型转换


S3本身不提供数据类型，该类型是DataX S3Reader定义：

| DataX 内部类型| S3 数据类型    |
| -------- | -----  |
| Float    |Double |
| Long     |Long |
| Double   |Double|
| String   |String|
| Boolean  |Boolean |
| Date     |Date |
| Timestamp|Timestamp |
| String    |String |
| Decimal  |Decimal |

其中：

* S3 Long是指S3文本中使用整形的字符串表示形式，例如"19901219"。
* S3 Double是指S3文本中使用Double的字符串表示形式，例如"3.1415"。
* S3 Boolean是指S3文本中使用Boolean的字符串表示形式，例如"true"、"false"。不区分大小写。
* S3 Date是指S3文本中使用Date的字符串表示形式，例如"2014-12-31"，Date可以指定format格式。

## 3 性能报告

略

## 4 约束限制

略

## 6 FAQ

略

