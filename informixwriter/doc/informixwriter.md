# InformixWriter 插件文档

---

## 1 快速介绍

InformixWriter 插件实现了写入数据到 Informix 主库的目的表的功能。在底层实现上， InformixWriter 通过 JDBC 连接远程 Informix 数据库，并执行相应的 insert into ... 的 sql 语句将数据写入 Informix。 

InformixWriter 面向ETL开发工程师，他们使用 InformixWriter 从数仓导入数据到 Informix。同时 InformixWriter 亦可以作为数据迁移工具为DBA等用户提供服务。

Informix引用驱动版本如下：
```
<dependency>
    <groupId>com.ibm.informix</groupId>
    <artifactId>jdbc</artifactId>
    <version>4.50.3</version>
</dependency>
```

## 2 实现原理

InformixWriter 通过 DataX 框架获取 Reader 生成的协议数据，InformixWriter 通过 JDBC 连接远程 Informix 数据库，并执行相应的 insert into ... 的 sql 语句将数据写入 Informix。


## 3 功能说明

### 3.1 配置样例

* 配置一个写入Informix的作业。

```
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
                    "name": "streamreader",
                    "parameter": {
                        "column": [
                            {
                                "value": "DataX",
                                "type": "string"
                            },
                            {
                                "value": 19880808,
                                "type": "long"
                            },
                            {
                                "value": "1988-08-08 08:08:08",
                                "type": "date"
                            },
                            {
                                "value": true,
                                "type": "bool"
                            },
                            {
                                "value": "test",
                                "type": "bytes"
                            }
                        ],
                        "sliceRecordCount": 1000
                    }
                },
                "writer": {
                    "name": "informixwriter",
                    "parameter": {
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:informix-sqli://ip:port/database",
                                "table": [
                                    "table"
                                ]
                            }
                        ],
                        "username": "username",
                        "password": "password",
                        "table": "table",
                        "column": [
                            "*"
                        ],
                        "preSql": [
                            "delete from XXX;"
                        ]
                    }
                }
            }
        ]
    }
}
```


### 3.2 参数说明


* **jdbcUrl**

  * 描述：描述的是到对端数据库的JDBC连接信息，jdbcUrl按照Informix官方规范，并可以填写连接附件控制信息。请注意不同的数据库jdbc的格式是不同的，DataX会根据具体jdbc的格式选择合适的数据库驱动完成数据读取。
  
	- jdbc格式 jdbc:informix-sqli://ip:port/database
  
  * 必选：是 <br />
  
  * 默认值：无 <br />
   
* **username**
 
  * 描述：数据源的用户名 <br />
  * 必选：是 <br />
  * 默认值：无 <br />

* **password**

  * 描述：数据源指定用户名的密码 <br />
  * 必选：是 <br />
  * 默认值：无 <br />
 
* **table**
  
  * 描述：目标表名称，如果表的schema信息和上述配置username不一致，请使用schema.table的格式填写table信息。 <br />
  * 必选：是 <br />
  * 默认值：无 <br />
 
* **column**

  * 描述：所配置的表中需要同步的列名集合。以英文逗号（,）进行分隔。`我们强烈不推荐用户使用默认列情况` <br />

  * 必选：是 <br />
  * 默认值：无 <br />
  
* **preSql**

  * 描述：执行数据同步任务之前率先执行的sql语句，目前只允许执行一条SQL语句，例如清除旧数据。<br />  
  * 必选：否 <br />  
  * 默认值：无 <br />  

* **postSql**

  * 描述：执行数据同步任务之后执行的sql语句，目前只允许执行一条SQL语句，例如加上某一个时间戳。 <br />  
  * 必选：否 <br />  
  * 默认值：无 <br />  

* **batchSize**

  * 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与Informix的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。<br />
 
  * 必选：否 <br />
 
  * 默认值：1024 <br />
  
### 3.3 类型转换

目前InformixReader支持大部分Informix数据库类型如数字、字符等，但也存在部分个别类型没有支持的情况，请注意检查你的类型。
