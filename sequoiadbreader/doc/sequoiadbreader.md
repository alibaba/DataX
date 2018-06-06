### Datax SequoiaDBReader
#### 1 快速介绍

SequoiaDBReader 插件利用 SequoiaDB 的java驱动进行SequoiaDB的读操作

#### 2 实现原理

SequoiaDBReader通过Datax框架从SequoiaDB并行的读取数据，通过主控的JOB程序按照_id字段对SequoiaDB中的数据进行分片，并行读取，然后将SequoiaDB支持的类型通过逐一判断转换成Datax支持的类型。

#### 3 功能说明
* 该示例从SequoiaDB读数据到MongoDB。

	    {
            "job": {
                "content": [
                    {
                        "reader": {
                            "name": "sequoiadbreader",
                            "parameter": {
                                "address": ["localhost:11810"],
                                "collectionName": "cl",
                                "collectionSpaceName": "cs",
                                "column": [
                                    {
                                        "name": "null",
                                        "type": "null"
                                    },
                                    {
                                        "name": "arr",
                                        "type": "Array",
                                        "splitter": ",",
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
                        },
                        "writer": {
                            "name": "mongodbwriter",
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
                                        "type": "Array",
                                        "splitter": ",",
                                        "itemtype": "int"
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
                                "dbName": "cs",
                                "upsertInfo": {
                                    "isUpsert": "true",
                                    "upsertKey": "_id"
                                },
                                "userName": "",
                                "userPassword": ""
                            }
                        }
                    }
                ],
                "setting": {
                    "speed": {
                        "channel": "2"
                    }
                }
            }
        }
#### 4 参数说明

* address： SequoiaDB的数据地址信息，因为SequoiaDB可能是个集群，则ip端口信息需要以Json数组的形式给出。【必填】
* userName：SequoiaDB的用户名。【选填】
* userPassword： SequoiaDB的密码。【选填】
* collectionName： SequoiaDB的集合名。【必填】
* collectionSpaceName: SequoiaDB的集合空间名。【必填】
* column：SequoiaDB的文档列名。【必填】
* name：Column的名字。【必填】
* type：Column的类型。【选填】
* splitter：因为SequoiaDB支持数组类型，但是Datax框架本身不支持数组类型，所以SequoiaDB读出来的数组类型要通过这个分隔符合并成字符串。【必填】
* itemType: 数组元素的类型

#### 5 类型转换

| DataX 内部类型| SequoiaDB 数据类型    |
| -------- | -----  |
| Long     | int, Long |
| Double   | double |
| String   | string, array, objectId, object |
| Date     | date  |
| Boolean  | boolean |
| Bytes    | bytes |


#### 6 性能报告
#### 7 测试报告