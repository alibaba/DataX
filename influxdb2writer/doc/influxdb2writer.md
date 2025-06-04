# DataX Influxdb2Writer


---


## 1 快速介绍

Influxdb2Writer 插件实现了写入数据到 Influxdb2 库的功能。在底层实现上， MysqlWriter 通过 JDBC 连接远程 Mysql 数据库，并执行相应的 insert into ... 或者 ( replace into ...) 的 sql 语句将数据写入 Mysql，内部会分批次提交入库，需要数据库本身采用 innodb 引擎。

MysqlWriter 面向ETL开发工程师，他们使用 MysqlWriter 从数仓导入数据到 Mysql。同时 MysqlWriter 亦可以作为数据迁移工具为DBA等用户提供服务。


## 2 实现原理

使用了`influxdb-client-java`的influxdb2的客户端sdk

## 3 功能说明

### 3.1 配置样例

* 这里使用一份从mysql产生到 Influxdb2 导入的数据。

```json
{
	"job": {
		"setting": {
			"speed": {
				"channel":1
			},
			"errorLimit": {
				"record": 0,
				"percentage": 0.02
			}
		},
		"content": [
			{
				"reader": {
					"name": "mysqlreader",
					"parameter": {
						"username": "root",
						"password": "123456",
						"column": [],
						"connection": [
							{
								"jdbcUrl": ["jdbc:mysql://127.0.0.1:3306/test_db?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai&useSSL=false&autoReconnect=true"],
								"querySql": ["select tag, value, create_time from test_table"]
							}
						]
					}
				},
				"writer": {
					"name": "influxdb2writer",
					"parameter": {
						"url": "http://127.0.0.1:8086",
						"token": "test_token",
						"org": "test",
						"bucket": "test_bucket",
						"measurement":  "test_data",
						"tags": ["tag"],
						"fields": ["value"],
						"hasTs": false,
						"tsFormat": "yyyy-MM-dd HH:mm:ss",
						"batchSize": 1000
					}
				}
			}
		]
	}
}


```


### 3.2 参数说明

* **url**

	* Influxdb2 的地址 <br />

 	* 必选：是 <br />

	* 默认值：无 <br />

* **token**

	* 描述：Influxdb2 的令牌 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **org**

	* 描述：Influxdb2 的组织 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **bucket**

	* 描述：Influxdb2 的数据桶/库。

	* 必选：是 <br />

	* 默认值：无 <br />

* **measurement**

	* 描述：Influxdb2 的数据表。

	* 必选：是 <br />

	* 默认值：否 <br />

* **tags**

	* 描述: Influxdb2 的数据表标签字段。

	* 必须: 否

	* 默认值: 空

* **fields**

	* 描述：Influxdb2 的数据表普通字段。

	* 必选：是 <br />

	* 默认值：无 <br />

* **hasTs**

	* 描述：是否存在timestamp, 有则默认为最后一列，否则以插入时间为准。

	* 必选：否  <br />

	* 默认值：false <br />
  
* 默认查询的record顺序为 tags, fields, time ，顺序不可乱

* **tsFormat**

	* 描述：查询的数据格式，将转存到influxdb填充time。

	* 必选：否  <br />

	* 默认值：yyyy-MM-dd HH:mm:ss <br />

* 默认查询的record顺序为 tags, fields, time ，顺序不可乱

* **batchSize**

	* 描述：一次性批量提交的记录数大小 <br />

	* 必选：否 <br />

	* 默认值：1000 <br />
