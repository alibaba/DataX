### Datax SequoiaDBWriter
#### 1 快速介绍

SequoiaDBWriter 插件利用 SequoiaDB 的java驱动进行SequoiaDB的写操作。

#### 2 实现原理

SequoiaDBWriter通过Datax框架获取Reader生成的数据，然后将Datax支持的类型通过逐一判断转换成SequoiaDB支持的类型。

#### 3 功能说明
* 该示例从MongoDB读一份数据到SequoiaDB。

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
                                "address": ["localhost:27017"],
                                "collectionName": "cl",
                                "column": [
                                         {
                                        "name": "null",
                                        "type": "null"
                                    },
                                    {
                                        "name": "arr",
                                        "type": "array",
                                        "splitter":","
                                    },
                                    {
                                        "name": "object",
                                        "type": "document"
                                    }
                                    {
                                        "name": "date",
                                        "type": "date"
                                    },
                                    {
                                        "name": "bool",
                                        "type": "bool"
                                    },
                                    {
                                        "name": "objectId",
                                        "type": "objectId"
                                    },
                                    {
                                        "name": "str",
                                        "type": "string"
                                    },
                                    {
                                        "name": "float",
                                        "type": "double"
                                    },
                                    {
                                        "name": "long",
                                        "type": "long"
                                    },
                                    {
                                        "name": "int",
                                        "type": "long"
                                    }
                                ],
                                "dbName": "cs",
                                "userName": "",
                                "userPassword": ""
                            }
                        },
                       "writer": {
                            "name": "sequoiadbwriter",
                            "parameter": {
                                "address": ["localhost:11810"],
                                "collectionName": "copyCL",
                                "collectionSpaceName": "cs",
                                "column": [
                                    {
                                        "name": "null",
                                        "type": "null"
                                    },
                                    {
                                        "name": "arr",
                                        "type": "Array",
                                        "splitter":",",
                                        "itemType": "int"
                                    },
                                    {
                                        "name": "object",
                                        "type": "document"
                                    },
                                    {
                                        "name": "date",
                                        "type": "date"
                                    },
                                    {
                                        "name": "bool",
                                        "type": "bool"
                                    },
                                    {
                                        "name": "objectId",
                                        "type": "objectId"
                                    },
                                    {
                                        "name": "str",
                                        "type": "string"
                                    },
                                    {
                                        "name": "float",
                                        "type": "double"
                                    },
                                    {
                                        "name": "long",
                                        "type": "long"
                                    },
                                    {
                                        "name": "int",
                                        "type": "int"
                                    }
                                ],
                                "userName": "",
                                "userPassword": ""
                            }
                        }
                    }
                ]
        	}
        }

#### 4 参数说明

* address： SequoiaDB的数据地址信息，因为SequoiaDB可能是个集群，则ip端口信息需要以Json数组的形式给出。【必填】
* userName：SequoiaDB的用户名。【选填】
* userPassword： SequoiaDB的密码。【选填】
* collectionSpaceName: SequoiaDB的集合空间名。【必填】
* collectionName： SequoiaDB的集合名。【必填】
* column：MongoDB的文档列名。【必填】
* name：Column的名字。【必填】
* type：Column的类型。【选填】
* splitter：特殊分隔符，当且仅当要处理的字符串要用分隔符分隔为字符数组时，才使用这个参数，通过这个参数指定的分隔符，将字符串分隔存储到SequoiaDB的数组中。【选填】
* itemType: 数组元素的数据类型
#### 5 类型转换

| DataX 内部类型| SequoiaDB 数据类型    |
| -------- | -----  |
| Long     | int, Long |
| Double   | double |
| String   | string, array, object, objectId |
| Date     | date  |
| Boolean  | boolean |
| Bytes    | bytes |


#### 6 性能报告
#### 7 测试报告