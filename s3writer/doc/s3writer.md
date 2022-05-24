# DataX S3Writer 说明


------------


## 1 功能与限制

S3Writer实现了从DataX协议转为S3中的TEXTFILE|PARQUET|ORC|AVRO文件功能，S3本身是无结构化数据存储，S3Writer需要在如下几个方面增加:

1. 支持且仅支持写入TEXTFILE|PARQUET|ORC|AVRO的文件，且要求shema为一张二维表。
   
2. 支持类CSV格式文件，自定义分隔符。

3. 支持多种数据格式SNAPPY/LZ4/LZO/DEFLATE/XZ,同时支持常见压缩NONE/GZIP/BZIP/BZIP2等等,每种数据格式支持压缩略有不同。

4. 文件支持滚动，当文件大于某个size值或者行数值，文件不需要切换。

5. jdbc中password和S3中accessKey/secretKey均采用密文形式.(实现见com/alibaba/datax/common/util/Securet)。

6. 支持重试和恢复，当s3出现ConnectTimeoutException/433等异常，可重试。


## 2 功能说明

### 2.1 配置样例

```json
{
	"job": {
		"setting": {
			"speed": {
				"channel": 2
			}
		},
		"content": [
			{
				"reader": {
					"name": "postgresqlreader",
					"parameter": {
						"username": "postgres",
						"password": "871d1f7908f3f6f1fa28d1509069cb1d",
						"splitPk": "qw",
						"connection": [
							{
								"querySql": [
									"select * from test.table_name;"
								],
								"jdbcUrl": [
									"jdbc:postgresql://localhost:5432/postgres"
								]
							}
						]
					}
				},
				"writer": {
					"name": "s3writer",
					"parameter": {
						"accessKey": "******",
						"secretKey": "******",
						"bucket": "******",
						"path": "tmp/ww1/pt=20200508/",
						"compression":"SNAPPY",
						"stored":"PARQUET",
						"writeMode": "truncate",
						"column":[
							{
								"name": "column_1",
								"type": "bigint"
							},
							{
								"name": "column_1",
								"type": "bigint"
							},
							{
								"name": "column_3",
								"type": "string"
							},
							{
								"name": "column_4",
								"type": "string"
							},
							{
								"name": "column_5",
								"type": "decimal(10,2)"
							},
							{
								"name": "column_6",
								"type": "decimal(10,4)"
							},
							{
								"name": "column_7",
								"type": "string"
							},
							{
								"name": "column_8",
								"type": "bigint"
							},
							{
								"name": "column_9",
								"type": "bigint"
							},
							{
								"name": "column_10",
								"type": "boolean"
							}
						]
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

 	* 描述：S3Writer写入的文件名，S3使用文件名模拟目录的实现;文件和目录格式: 库名/表名/分区/文件名.格式。

	* 必选：是 <br />

	* 默认值：无 <br />
 
* **writeMode**

 	* 描述：S3Writer写入前数据清理处理： <br />

		* truncate，写入前清理object名称前缀匹配的所有object。例如: "object": "abc"，将清理所有abc开头的object。
		* append，写入前不做任何处理.

	* 必选：是 <br />

	* 默认值：append <br />
	
* **column**

	* 描述：s3写出时的schema，示例如下。 <br />
	  
	* 必选：是 <br />

	* 默认值：无 <br />
```json
[
	{
		"name": "column_4",
		"type": "bigint"
	},
	{
		"name": "column_5",
		"type": "string"
	},
	{
		"name": "column_6",
		"type": "decimal(10,4)"
	},
	{
		"name": "column_7",
		"type": "string"
	},
	{
		"name": "column_8",
		"type": "data"
	},
	{
		"name": "column_9",
		"type": "timestamp"
	},
	{
		"name": "column_10",
		"type": "boolean"
	}
]
```

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

S3本身不提供数据类型，该类型是DataX S3Writer定义(所有数据格式均支持)：

| DataX 内部类型| S3 数据类型    |
| -------- | -----  |
| Float    |Double |
| Long     |Long |
| Double   |Double|
| String   |String|
| Boolean  |Boolean |
| Date     |Long/Date |
| Timestamp|Long/Timestamp |
| Map      |String |
| List     |String |
| Array    |String |
| Decimal  |Decimal |


### 2.4 重试和恢复

不可恢复的问题
* 没有对象/存储桶存储：FileNotFoundException
* 没有访问权限：AccessDeniedException
* 网络错误被视为不可恢复（UnknownHostException、NoRouteToHostException、AWSRedirectException）。
* 中断：InterruptedIOException，InterruptedException。
* 被拒绝的 HTTP 请求：InvalidRequestException

可能恢复和重试的问题

* 连接超时：ConnectTimeoutException。设置与 S3 端点（或代理）的连接之前超时
* EOFException : 读取数据时连接中断
* 服务器无响应 (443, 444) HTTP 响应

注意点：
* 失败将使用fs.s3a.retry.interval 中设置间隔重试
* 直到fs.s3a.retry.limit 中设置的限制次数
* S3保证最终一致，并且不支持原子的 create-no-overwrite 操作
* 默认值
```xml
<property>
	<name>fs.s3a.retry.interval</name>
	<value>500ms</value>
	<description>重试间隔时间</description>
</property>
```
```xml
<property>
<name>fs.s3a.retry.throttle.interval</name>
<value>${fs.s3a.attempts.maximum}</value>
<description>重试次数，默认20次</description>
</property>
```

### 2.5文件滚动

* 将大文件作为块上传，大小由fs.s3a.multipart.size设置。即：分段上传开始的阈值和每次上传的大小相同
* 将块缓冲到磁盘（默认）或在堆上或堆外内存中
* 在后台线程中并行上传块
* 一旦缓冲数据超过此分区大小，就开始上传块
* 将数据缓冲到磁盘时，使用fs.s3a.buffer.dir 中列出的目录。可以缓冲的数据大小受限于可用磁盘空间


```xml
<property>
	<name>fs.s3a.fast.upload</name>
	<value>true</value>
	<description>开启快速上传（文件滚动）,默认true</description>
</property> 
```

```xml
<property>
	<name>fs.s3a.fast.upload.buffer</name>
	<value>disk</value>
	<description>
		要使用的缓冲机制。
		值：磁盘、数组、字节缓冲区。

		“磁盘”将使用 fs.s3a.buffer.dir 中列出的目录
		作为上传前保存数据的位置。

		“array”使用JVM堆中的数组

		“bytebuffer”使用JVM内的堆外内存。

		“array”和“bytebuffer”都将在单个流中消耗内存，最多可达以下
		设置的块数：

		fs.s3a.multipart.size * fs.s3a.fast.upload.active.blocks。

		如果使用这两种机制中的任何一种，

		跨所有线程执行工作的线程总数由
		fs.s3a.threads.max设置，fs.s3a.max.total.tasks 值设置排队
		工作项的数量。
	</description>
</property> 
```

```xml
<property>
	<name>fs.s3a.multipart.size</name>
	<value>100M</value>
	<description>将上传或复制文件拆分(K,M,G,T,P),默认100m</description>
</property> 
```

注意点：

* 如果写入流的数据量低于fs.s3a.multipart.size中设置的数据量，则在OutputStream.close()操作中执行上传- 与原始输出流一样。
* 在close()调用中写入完成之前，正在写入的文件仍然不可见，该调用将阻塞，直到上传完成。

## 3 性能报告

略

## 4 约束限制

略

## 5 FAQ

略

