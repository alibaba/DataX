# DataX SybaseWriter


---


## 1 快速介绍

SybaseWriter 插件实现了写入数据到 Sybase 主库的目的表的功能。在底层实现上， SybaseWriter 通过 JDBC 连接远程 Sybase 数据库，并执行相应的 insert into ...  sql 语句将数据写入 Sybase，内部会分批次提交入库。

SybaseWriter 面向ETL开发工程师，他们使用 SybaseWriter 从数仓导入数据到 Sybase。同时 SybaseWriter 亦可以作为数据迁移工具为DBA等用户提供服务。


## 2 实现原理

SybaseWriter 通过 DataX 框架获取 Reader 生成的协议数据，根据你配置生成相应的SQL语句


* `insert into...`(当主键/唯一性索引冲突时会写不进去冲突的行)

<br />

    注意：
    1. 目的表所在数据库必须是主库才能写入数据；整个任务至少需具备 insert into...的权限，是否需要其他权限，取决于你任务配置中在 preSql 和 postSql 中指定的语句。
    2.SybaseWriter和MysqlWriter不同，不支持配置writeMode参数。


## 3 功能说明

### 3.1 配置样例

* 这里使用一份从内存产生到 Sybase 导入的数据。

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
                    "name": "streamreader",
                    "parameter": {
                        "column" : [
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
                    "name": "SybaseWriter",
                    "parameter": {
                        "username": "root",
                        "password": "root",
                        "column": [
                            "id",
                            "name"
                        ],
                        "preSql": [
                            "delete from test"
                        ],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:sybase:Tds:[HOST_NAME]:PORT/[DATABASE_NAME]",
                                "table": [
                                    "test"
                                ]
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

* **jdbcUrl**

    * 描述：目的数据库的 JDBC 连接信息 ,jdbcUrl必须包含在connection配置单元中。

               注意：1、在一个数据库上只能配置一个值。这与 SybaseReader 支持多个备库探测不同，因为此处不支持同一个数据库存在多个主库的情况(双主导入数据情况)


  * 必选：是 <br />

  * 默认值：无 <br />

* **username**

  * 描述：目的数据库的用户名 <br />

  * 必选：是 <br />

  * 默认值：无 <br />

* **password**

  * 描述：目的数据库的密码 <br />

  * 必选：是 <br />

  * 默认值：无 <br />

* **table**

  * 描述：目的表的表名称。支持写入一个或者多个表。当配置为多张表时，必须确保所有表结构保持一致。

               注意：table 和 jdbcUrl 必须包含在 connection 配置单元中

  * 必选：是 <br />

  * 默认值：无 <br />

* **column**

  * 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。如果要依次写入全部列，使用*表示, 例如: "column": ["*"]

    		**column配置项必须指定，不能留空！**


               注意：1、我们强烈不推荐你这样配置，因为当你目的表字段个数、类型等有改动时，你的任务可能运行不正确或者失败
                    2、此处 column 不能配置任何常量值

  * 必选：是 <br />

  * 默认值：否 <br />

* **preSql**

  * 描述：写入数据到目的表前，会先执行这里的标准语句。如果 Sql 中有你需要操作到的表名称，请使用 `@table` 表示，这样在实际执行 Sql 语句时，会对变量按照实际表名称进行替换。比如你的任务是要写入到目的端的100个同构分表(表名称为:datax_00,datax01, ... datax_98,datax_99)，并且你希望导入数据前，先对表中数据进行删除操作，那么你可以这样配置：`"preSql":["delete from @table"]`，效果是：在执行到每个表写入数据前，会先执行对应的 delete from 对应表名称 <br />

  * 必选：否 <br />

  * 默认值：无 <br />

* **postSql**

  * 描述：写入数据到目的表后，会执行这里的标准语句。（原理同 preSql ） <br />

  * 必选：否 <br />

  * 默认值：无 <br />

* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与SybaseReader的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。<br />

	* 必选：否 <br />

	* 默认值：1024 <br />



### 3.3 类型转换(同SybaseReader待测试验证后陆续更新支持类型)




| DataX 内部类型| Sybase 数据类型    |
| -------- | -----  |
| Long     |NUMBER,INTEGER,INT,SMALLINT|
| Double   |NUMERIC,DECIMAL,FLOAT,DOUBLE PRECISION,REAL|
| String   |LONG,CHAR,NCHAR,VARCHAR,VARCHAR2,NVARCHAR2,CLOB,NCLOB,CHARACTER,CHARACTER VARYING,CHAR VARYING,NATIONAL CHARACTER,NATIONAL CHAR,NATIONAL CHARACTER VARYING,NATIONAL CHAR VARYING,NCHAR VARYING    |
| Date     |TIMESTAMP,DATE    |
| Boolean  |bit, bool   |
| Bytes    |BLOB,BFILE,RAW,LONG RAW    |



