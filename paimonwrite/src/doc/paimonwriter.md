
# PaimonWriter 插件文档


___



## 1 快速介绍

PaimonWriter插件实现了向数据湖Paimon中写入数据，在底层实现上，通过调用paimon的batch write和stream write的相关方法来讲数据写入到paimon中

## 2 实现原理

通过读取paimon的文件catalog或者hive catalog的路径，以及相关hadoop配置，hive配置等信息来写入数据 元数据文件等信息到文件系统中

## 3 功能说明

### 3.1 配置样例

* 配置一个从mysql到paimon导入的作业:

```
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
                    "name": "mysqlreader",
                    "parameter": {
                        "column": [
                            "id",
                            "name",
                            "age",
                            "score",
                            "create_at",
                            "update_at",
                            "dt"
                        ],
                        "connection": [
                            {
                                "jdbcUrl": [
                                    "jdbc:mysql://127.0.0.1:3306/demo?useSSL=false&useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai"
                                ],
                                "table": [
                                    "user"
                                ]
                            }
                        ],
                        "password": "root1234",
                        "username": "root",
                        "where": ""
                    }
                },
                "writer": {
                    "name": "paimonwriter",
                    "parameter": {
                        "tableName": "test",
                        "databaseName": "paimon",
                        "catalogPath": "/app/hive/warehouse",
                        "metastoreUri": "thrift://127.0.0.1:9083",
                        "hiveConfDir": "/your/path",
                        "catalogType": "hive",
                        "hiveConfDir": "/your/path",
                        "hadoopConfDir": "/your/path",
                        "tableBucket": 2,
                        "primaryKey": "dt,id",
                        "partitionFields": "dt",
                        "writeOption": "stream_insert",
                        "batchSize": 100,
                        "hadoopConfig": {
                            "hdfsUser": "hdfs",
                            "coreSitePath": "/your/path/core-site.xml",
                            "hdfsSitePath": "/your/path/hdfs-site.xml"
                        },
                        "paimonConfig": {
                            "compaction.min.file-num": "3",
                            "compaction.max.file-num": "6",
                            "snapshot.time-retained": "2h",
                            "snapshot.num-retained.min": "5",
                            "hive.table.owner": "zhangsan",
                            "hive.storage.format": "ORC"
                        },
                        "column": [
                            {
                                "name": "id",
                                "type": "int"
                            },
                            {
                                "name": "name",
                                "type": "string"
                            },
                            {
                                "name": "age",
                                "type": "int"
                            },
                            {
                                "name": "score",
                                "type": "double"
                            },
                            {
                                "name": "create_at",
                                "type": "string"
                            },
                            {
                                "name": "update_at",
                                "type": "string"
                            },{
                                "name": "dt",
                                "type": "string"
                            }
                        ]
                    }
                }
            }
        ]
    }
}

```


### 3.2 参数说明

* **metastoreUri**

	* 描述：需要配置hive的metastore地址:thrift://127.0.0.1:9083,注意:当设置了metastoreUri,则不需要设置hiveConfDir。 <br />

	* 必选：metastoreUri和hiveConfDir配置二选一 <br />

	* 默认值：无 <br />
	
* **hiveConfDir**

	* 描述：如果没有设置hive的metastoreUri,则需要设置hiveConfDir路径，注意：路径中必须要包含hive-site.xml文件。 <br />

	* 必选：metastoreUri和hiveConfDir配置二选一 <br />

	* 默认值：无 <br />

* **catalogPath**

	* 描述：catalogPath是paimon创建的catalog路径，可以包含文件系统的和hdfs系统的路径。 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **catalogType**

	* 描述：paimon的catalog类型，支持两种选项，1.file,2.hive <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **hadoopConfDir**

	* 描述：paimon依赖的hadoop文件配置路径，注意：路径下面要包含两个文件:hdfs-site.xml,core-site.xml <br />

	* 必选：hadoopConfDir和hadoopConfig下的coreSitePath,hdfsSitePath配置二选一 <br />

	* 默认值：无 <br />

* **writeOption**

	* 描述：paimon写入数据的方式，目前支持2种方式：1.batch_insert(按照官方的定义模式,每次只能有一次提交)，2.stream_insert(支持多次提交) <br />

	* 必选：是 <br />

	* 默认值：false <br />

* **hadoopConfig**

	* 描述：设置hadoop的配置参数，可以以设置配置文件core-site.xml和hdfs-site.xml以及可配置kerberos和s3相关参数。<br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **paimonConfig**

	* 描述：paimon的相关配置信息都可以加入。<br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **keyspace**

	* 描述：需要同步的表所在的keyspace。<br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **table**

	* 描述：所选取的需要同步的表。<br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：所配置的表中需要同步的列集合。<br />
	  内容可以是列的名称或"writetime()"。如果将列名配置为writetime()，会将这一列的内容作为时间戳。

	* 必选：是 <br />

	* 默认值：无 <br />
	
* **bucket**

	* 描述：paimon设置bucket大小，注意如果设置为-1则会出现，无法动态的写入分区错误：<br />

	* 必选：否 <br />

	* 默认值：2 <br />

* **batchSize**

	* 描述：一次批量提交(BATCH)的记录条数，注意：次配置是配合在stream_insert模式下使用的，其他模式无效：<br />

	* 必选：否 <br />

	* 默认值：10 <br />


### 3.3 类型转换

| DataX 内部类型| paimon 数据类型    |
| -------- | -----  |
| Long     |long|
| float     |float|
| float     |float|
| decimal   |decimal|
| String   |string   |
| Date     |date, timestamp,datatime, string   |
| Boolean  |boolean   |


请注意:

* 目前不支持union,row,struct类型和custom类型。

## 4 性能报告

略

## 5 约束限制

### 5.1 主备同步数据恢复问题

略

## 6 FAQ



