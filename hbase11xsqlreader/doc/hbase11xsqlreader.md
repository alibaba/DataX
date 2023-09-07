# hbase11xsqlreader  插件文档


___


 
## 1 快速介绍

hbase11xsqlreader插件实现了从Phoenix(HBase SQL)读取数据。在底层实现上，hbase11xsqlreader通过Phoenix客户端去连接远程的HBase集群，并执行相应的sql语句将数据从Phoenix库中SELECT出来。


## 2 实现原理

简而言之，hbase11xsqlreader通过Phoenix客户端去连接远程的HBase集群，并根据用户配置的信息生成查询SELECT 语句，然后发送到HBase集群，并将返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。
# hbase11xsqlreader  插件文档


___


 
## 1 快速介绍

hbase11xsqlreader插件实现了从Phoenix(HBase SQL)读取数据。在底层实现上，hbase11xsqlreader通过Phoenix客户端去连接远程的HBase集群，并执行相应的sql语句将数据从Phoenix库中SELECT出来。


## 2 实现原理

简而言之，hbase11xsqlreader通过Phoenix客户端去连接远程的HBase集群，并根据用户配置的信息生成查询SELECT 语句，然后发送到HBase集群，并将返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。


## 3 功能说明

### 3.1 配置样例

* 配置一个从Phoenix同步抽取数据到本地的作业:

```
{
    "job": {
        "setting": {
            "speed": {
            //设置传输速度，单位为byte/s，DataX运行会尽可能达到该速度但是不超过它.
                "byte":10485760
            },  
            //出错限制
            "errorLimit": {
                //出错的record条数上限，当大于该值即报错。
                "record": 0,
                //出错的record百分比上限 1.0表示100%，0.02表示2%
                "percentage": 0.02
            }   
        },  
        "content": [ { 
                "reader": {
                     //指定插件为hbase11xsqlreader
                    "name": "hbase11xsqlreader",
                    "parameter": {
                         //填写连接Phoenix的hbase集群zk地址
                        "hbaseConfig": {
                            "hbase.zookeeper.quorum": "hb-proxy-xxx-002.hbase.rds.aliyuncs.com,hb-proxy-xxx-001.hbase.rds.aliyuncs.com,hb-proxy-xxx-003.hbase.rds.aliyuncs.com"
                        }, 
                        //填写要读取的phoenix的命名空间
                        "schema": "TAG",
                        //填写要读取的phoenix的表名
                        "table": "US_POPULATION",
                        //填写要读取的列名，不填读取所有列
                        "column": [
                        ],
                        //查询条件
                       "where": "id="
                    }   
                },  
                "writer": {
                    //writer类型
                    "name": "streamwriter",
                     //是否打印内容
                    "parameter": {
                        "print":true,
                        "encoding": "UTF-8"
                    }   
                }   
            }   
        ]   
    }   
}
```


### 3.2 参数说明

* **hbaseConfig**

	* 描述：hbase11xsqlreader需要通过Phoenix客户端去连接hbase集群，因此这里需要填写对应hbase集群的zkurl地址，注意不要添加2181。
 
	* 必选：是 <br />
 
	* 默认值：无 <br />
* **schema**
 
	* 描述：编写Phoenix中的namespace，该值设置为''
 
	* 必选：是 <br />
 
	* 默认值：无 <br />
 
* **table**
 
	* 描述：编写Phoenix中的表名，该值设置为'tablename'
 
	* 必选：是 <br />
 
	* 默认值：无 <br />

* **column**

	* 描述：填写需要从phoenix表中读取的列名集合，使用JSON的数组描述字段信息，空值表示读取所有列。
 
	* 必选：是 <br />
 
	* 默认值：无 <br />
* **where**

	* 描述：填写需要从phoenix表中读取条件判断。
 
	* 可选：是 <br />
 
	* 默认值：无 <br />

### 3.3 类型转换

目前hbase11xsqlreader支持大部分Phoenix类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出MysqlReader针对Mysql类型转换列表:


| DataX 内部类型| Phoenix 数据类型    |
| -------- | -----  |
| String     |CHAR, VARCHAR|
| Bytes   |BINARY, VARBINARY|
| Bool   |BOOLEAN   | 
| Long     |INTEGER, TINYINT, SMALLINT, BIGINT  | 
| Double  |FLOAT, DECIMAL, DOUBLE,   |  
| Date    |DATE, TIME, TIMESTAMP    | 



## 4 性能报告

略

## 5 约束限制
略
## 6 FAQ

***



## 3 功能说明

### 3.1 配置样例

* 配置一个从Phoenix同步抽取数据到本地的作业:

```
{
    "job": {
        "setting": {
            "speed": {
            //设置传输速度，单位为byte/s，DataX运行会尽可能达到该速度但是不超过它.
                "byte":10485760
            },  
            //出错限制
            "errorLimit": {
                //出错的record条数上限，当大于该值即报错。
                "record": 0,
                //出错的record百分比上限 1.0表示100%，0.02表示2%
                "percentage": 0.02
            }   
        },  
        "content": [ { 
                "reader": {
                     //指定插件为hbase11xsqlreader
                    "name": "hbase11xsqlreader",
                    "parameter": {
                         //填写连接Phoenix的hbase集群zk地址
                        "hbaseConfig": {
                            "hbase.zookeeper.quorum": "hb-proxy-xxx-002.hbase.rds.aliyuncs.com,hb-proxy-xxx-001.hbase.rds.aliyuncs.com,hb-proxy-xxx-003.hbase.rds.aliyuncs.com"
                        },  
                        "schema": "TAG",
                        //填写要读取的phoenix的表名
                        "table": "US_POPULATION",
                        //填写要读取的列名，不填读取所有列
                        "column": [
                        ],
                        //查询条件
                       "where": "id="
                    }   
                },  
                "writer": {
                    //writer类型
                    "name": "streamwriter",
                     //是否打印内容
                    "parameter": {
                        "print":true,
                        "encoding": "UTF-8"
                    }   
                }   
            }   
        ]   
    }   
}
```


### 3.2 参数说明

* **hbaseConfig**

	* 描述：hbase11xsqlreader需要通过Phoenix客户端去连接hbase集群，因此这里需要填写对应hbase集群的zkurl地址，注意不要添加2181。
 
	* 必选：是 <br />
 
	* 默认值：无 <br />
* **schema**
  
 	* 描述：编写Phoenix中的namespace，该值设置为''
  
 	* 必选：是 <br />
  
 	* 默认值：无 <br />
* **table**
 
	* 描述：编写Phoenix中的表名,如果有namespace，该值设置为'namespace.tablename'
 
	* 必选：是 <br />
 
	* 默认值：无 <br />

* **column**

	* 描述：填写需要从phoenix表中读取的列名集合，使用JSON的数组描述字段信息，空值表示读取所有列。
 
	* 必选：是 <br />
 
	* 默认值：无 <br />
 * **where**
 
 	* 描述：填写需要从phoenix表中读取条件判断。
  
 	* 可选：是 <br />
  
 	* 默认值：无 <br />

### 3.3 类型转换

目前hbase11xsqlreader支持大部分Phoenix类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出MysqlReader针对Mysql类型转换列表:


| DataX 内部类型| Phoenix 数据类型    |
| -------- | -----  |
| String     |CHAR, VARCHAR|
| Bytes   |BINARY, VARBINARY|
| Bool   |BOOLEAN   | 
| Long     |INTEGER, TINYINT, SMALLINT, BIGINT  | 
| Double  |FLOAT, DECIMAL, DOUBLE,   |  
| Date    |DATE, TIME, TIMESTAMP    | 



## 4 性能报告

略

## 5 约束限制
略
## 6 FAQ

***

