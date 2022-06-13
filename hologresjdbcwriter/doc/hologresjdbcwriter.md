# DataX HologresJdbcWriter


---


## 1 快速介绍

HologresJdbcWriter 插件实现了写入数据到 Hologres目的表的功能。在底层实现上，HologresJdbcWriter通过JDBC连接远程 Hologres 数据库，并执行相应的 insert into ... on conflict sql 语句将数据写入 Hologres，内部会分批次提交入库。

<br />

* HologresJdbcWriter 只支持单表同步

## 2 实现原理

HologresJdbcWriter 通过 DataX 框架获取 Reader 生成的协议数据，根据你配置生成相应的SQL插入语句

* `insert into... on conflict `


## 3 功能说明

### 3.1 配置样例

* 这里使用一份从内存产生到 HologresJdbcWriter导入的数据。

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
                    "name": "hologresjdbcwriter",
                    "parameter": {
                        "username": "xx",
                        "password": "xx",
                        "column": [
                            "id",
                            "name"
                        ],
                        "preSql": [
                            "delete from test"
                        ],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:postgresql://127.0.0.1:3002/datax",
                                "table": [
                                    "test"
                                ]
                            }
                        ],
                        "writeMode" : "REPLACE",
                        "client" : {
                            "writeThreadSize" : 3
                        }
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

      注意：1、在一个数据库上只能配置一个值。
      2、jdbcUrl按照PostgreSQL官方规范，并可以填写连接附加参数信息。具体请参看PostgreSQL官方文档或者咨询对应 DBA。


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

  * 描述：目的表的表名称。只支持写入一个表。

               注意：table 和 jdbcUrl 必须包含在 connection 配置单元中

  * 必选：是 <br />

  * 默认值：无 <br />

* **column**

  * 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。如果要依次写入全部列，使用\*表示, 例如: "column": ["\*"]

               注意：1、我们强烈不推荐你这样配置，因为当你目的表字段个数、类型等有改动时，你的任务可能运行不正确或者失败
                    2、此处 column 不能配置任何常量值

  * 必选：是 <br />

  * 默认值：否 <br />

* **preSql**

  * 描述：写入数据到目的表前，会先执行这里的标准语句。如果 Sql 中有你需要操作到的表名称，请使用 `@table` 表示，这样在实际执行 Sql 语句时，会对变量按照实际表名称进行替换。 <br />

  * 必选：否 <br />

  * 默认值：无 <br />

* **postSql**

  * 描述：写入数据到目的表后，会执行这里的标准语句。（原理同 preSql ） <br />

  * 必选：否 <br />

  * 默认值：无 <br />

* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与HologresJdbcWriter的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。<br />

	* 必选：否 <br />

	* 默认值：512 <br />

* **writeMode**

	* 描述：当写入hologres有主键表时，控制主键冲突后的策略。REPLACE表示冲突后hologres表的所有字段都被覆盖（未在writer中配置的字段将填充null);UPDATE表示冲突后hologres表writer配置的字段将被覆盖；IGNORE表示冲突后丢弃新数据，不覆盖。 <br />

	* 必选：否 <br />

	* 默认值：REPLACE <br />

* **client.writeThreadSize**

	* 描述：写入hologres的连接池大小，多个连接将并行写入数据。 <br />

	* 必选：否 <br />

	* 默认值：1 <br />
	
### 3.3 类型转换

目前 HologresJdbcWriter支持大部分 Hologres类型，但也存在部分没有支持的情况，请注意检查你的类型。

下面列出 HologresJdbcWriter针对 Hologres类型转换列表:

| DataX 内部类型| Hologres 数据类型    |
| -------- | -----  |
| Long     |bigint, integer, smallint |
| Double   |double precision, money, numeric, real |
| String   |varchar, char, text, bit|
| Date     |date, time, timestamp |
| Boolean  |bool|
| Bytes    |bytea|
