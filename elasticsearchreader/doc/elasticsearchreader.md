# DataX ElasticSearchReader


---

## 1 快速介绍

数据导出elasticsearch的插件，本插件仅在Elasticsearch 5.x上测试

## 2 实现原理

使用elasticsearch-rest-high-level-client读取elasticsearch

## 3 功能说明

### 3.1 配置样例

#### job.json

```
{
	"job": {
		"setting": {
			"speed": {
				"channel": 5
			}
		},
		"content": [{
			"reader": {
				"name": "elasticsearchreader",
				"parameter": {
					"esClusterAddress": ["127.0.0.1:9288"],
					"esIndex": "xxxx",
					"esType": "xxxx",
					"username": "xxx",
					"password": "xxx",
					"batchSize": "10000",
					"query": "{\"range\": {\"createDate\": {\"gte\": \"2020-03-21T00:00:00.000Z\", \"lte\": \"2020-03-22T00:00:00.000Z\" }}}",
					"column": [{
						"name": "exception",
						"type": "STRING"
					}, {
						"name": "createBy",
						"type": "STRING"
					}, {
						"name": "method",
						"type": "STRING"
					}, {
						"name": "userAgent",
						"type": "STRING"
					}, {
						"name": "requestUri",
						"type": "STRING"
					}, {
						"name": "params",
						"type": "STRING"
					}, {
						"name": "title",
						"type": "STRING"
					}, {
						"name": "type",
						"type": "STRING"
					}, {
						"name": "remoteAddr",
						"type": "STRING"
					}, {
						"name": "createDate",
						"type": "STRING"
					}]

				}
			},
			"writer": {
				"name": "txtfilewriter",
				"parameter": {
					"path": "/synEs/data/20200101",
					"fileName": "sys_log",
					"writeMode": "truncate",
					"fieldDelimiter": ",",
					"datetimeFormat": "yyyy-MM-dd HH:mm:ss",
					"dateFormat": "yyyy-MM-dd HH:mm:ss"
				}
			}
		}]
	}
}
```

#### 3.2 参数说明

* esClusterAddress
  * 描述：es集群连接地址
  * 必选：是
  * 默认值：无

* esIndex
  * 描述：es的索引名称
  * 必选：是
  * 默认值：空

* esType
  * 描述：es的type
  * 必选：否
  * 默认值：空
* username
  * 描述：es的用户名
  * 必选：否
  * 默认值：空
      
* password
  * 描述：es的秘密
  * 必选：否
  * 默认值：空
  
* batchSize
  * 描述：每次批量数据的条数
  * 必选：否
  * 默认值：10000

* keepAlive
  * 描述：es的scrollId过期时间，单位分钟
  * 必选：否
  * 默认值：1分钟
 
* query
  * 描述：es的查询条件
  * 必选：否
  * 默认值：空

* column
  * 描述：elasticsearch所需要同步的数据字段名称
  * 必选：是


## 4 性能报告

## 5 约束限制

** elasticsearch reader的channel个数不要超过es的shard数