### Datax MongoDBReader
#### 1 快速介绍

MongoDBReader 插件利用 MongoDB 的java客户端MongoClient进行MongoDB的读操作。最新版本的Mongo已经将DB锁的粒度从DB级别降低到document级别，配合上MongoDB强大的索引功能，基本可以达到高性能的读取MongoDB的需求。

#### 2 实现原理

MongoDBReader通过Datax框架从MongoDB并行的读取数据，通过主控的JOB程序按照指定的规则对MongoDB中的数据进行分片，并行读取，然后将MongoDB支持的类型通过逐一判断转换成Datax支持的类型。

#### 3 功能说明
* 该示例从MongoDB读一份数据到ODPS。

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
	                    "name": "mongodbreader",
	                    "parameter": {
	                        "address": ["127.0.0.1:27017"],
	                        "userName": "",
	                        "userPassword": "",
	                        "dbName": "tag_per_data",
	                        "collectionName": "tag_data12",
	                        "column": [
	                            {
	                                "name": "unique_id",
	                                "type": "string"
	                            },
	                            {
	                                "name": "sid",
	                                "type": "string"
	                            },
	                            {
	                                "name": "user_id",
	                                "type": "string"
	                            },
	                            {
	                                "name": "auction_id",
	                                "type": "string"
	                            },
	                            {
	                                "name": "content_type",
	                                "type": "string"
	                            },
	                            {
	                                "name": "pool_type",
	                                "type": "string"
	                            },
	                            {
	                                "name": "frontcat_id",
	                                "type": "Array",
	                                "spliter": ""
	                            },
	                            {
	                                "name": "categoryid",
	                                "type": "Array",
	                                "spliter": ""
	                            },
	                            {
	                                "name": "gmt_create",
	                                "type": "string"
	                            },
	                            {
	                                "name": "taglist",
	                                "type": "Array",
	                                "spliter": " "
	                            },
	                            {
	                                "name": "property",
	                                "type": "string"
	                            },
	                            {
	                                "name": "scorea",
	                                "type": "int"
	                            },
	                            {
	                                "name": "scoreb",
	                                "type": "int"
	                            },
	                            {
	                                "name": "scorec",
	                                "type": "int"
	                            }
	                        ]
	                    }
	                },
	                "writer": {
	                    "name": "odpswriter",
	                    "parameter": {
	                        "project": "tb_ai_recommendation",
	                        "table": "jianying_tag_datax_read_test01",
	                        "column": [
	                            "unique_id",
	                            "sid",
	                            "user_id",
	                            "auction_id",
	                            "content_type",
	                            "pool_type",
	                            "frontcat_id",
	                            "categoryid",
	                            "gmt_create",
	                            "taglist",
	                            "property",
	                            "scorea",
	                            "scoreb"
	                        ],
	                        "accessId": "**************",
	                        "accessKey": "********************",
	                        "truncate": true,
	                        "odpsServer": "xxx/api",
	                        "tunnelServer": "xxx"
	                    }
	                }
	            }
	        ]
	    }
        }
#### 4 参数说明

* address： MongoDB的数据地址信息，因为MonogDB可能是个集群，则ip端口信息需要以Json数组的形式给出。【必填】
* userName：MongoDB的用户名。【选填】
* userPassword： MongoDB的密码。【选填】
* authDb: MongoDB认证数据库【选填】
* collectionName： MonogoDB的集合名。【必填】
* column：MongoDB的文档列名。【必填】
* name：Column的名字。【必填】
* type：Column的类型。【选填】
* splitter：因为MongoDB支持数组类型，但是Datax框架本身不支持数组类型，所以mongoDB读出来的数组类型要通过这个分隔符合并成字符串。【选填】
* query: MongoDB的额外查询条件。【选填】

#### 5 类型转换

| DataX 内部类型| MongoDB 数据类型    |
| -------- | -----  |
| Long     | int, Long |
| Double   | double |
| String   | string, array |
| Date     | date  |
| Boolean  | boolean |
| Bytes    | bytes |


#### 6 性能报告
#### 7 测试报告
