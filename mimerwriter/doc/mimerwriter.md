# DataX MimerWriter


---


## 1 快速介绍

MimerWriter插件实现了写入数据到 Mimer SQL数据库目的表的功能。在底层实现上，MimerWriter通过JDBC连接远程 Mimer SQL 数据库，并执行相应的 insert into ... sql 语句将数据写入。


## 2 实现原理

MimerWriter通过 DataX 框架获取 Reader 生成的协议数据，根据你配置的 `writeMode` 生成


* `insert into...`(`writeMode`为insert，当主键/唯一性索引冲突时会写不进去冲突的行)

##### 或者

* `insert {replace} into ...`(`writeMode`为replace，没有遇到主键/唯一性索引冲突时，与 insert into 行为一致，冲突时会用新行替换原有行所有字段) 

##### 或者

* `insert {ignore} into ...`(`writeMode`为ignore，没有遇到主键/唯一性索引冲突时，与 insert into 行为一致，冲突时会用忽略插入操作) 


的语句写入数据到 Mimer SQL。

<br />

    注意：
    整个任务至少需具备 insert into...的权限，是否需要其他权限，取决于你任务配置中在 preSql 和 postSql 中指定的语句。


## 3 功能说明

### 3.1 配置样例

* 这里使用一份从内存产生到 MimerWriter导入的数据。

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
                    "name": "mimerwriter",
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
                                "jdbcUrl": "jdbc:mimer://localhost:1360/datax",
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

      注意：1、在一个数据库上只能配置一个值。
      2、jdbcUrl按照Mimer SQL官方规范，并可以填写连接附加参数信息。具体请参看Mimer SQL官方文档(https://developer.mimer.com/article/jdbc/)或者咨询对应 DBA。


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

  * 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。如果要依次写入全部列，使用`*`表示, 例如: `"column": ["*"]`

               注意：1、我们强烈不推荐你这样配置，因为当你目的表字段个数、类型等有改动时，你的任务可能运行不正确或者失败
                    2、此处 column 不能配置任何常量值

  * 必选：是 <br />

  * 默认值：否 <br />

* **preSql**

  * 描述：写入数据到目的表前，会先执行这里的标准语句。<br />

  * 必选：否 <br />

  * 默认值：无 <br />

* **postSql**

  * 描述：写入数据到目的表后，会执行这里的标准语句。（原理同 preSql ） <br />

  * 必选：否 <br />

  * 默认值：无 <br />

* **writeMode**

	* 描述：控制写入数据到目标表采用 `insert into` 或者 `insert {replace} into` 或者 `insert {ignore} into` 语句<br />

	* 必选：是 <br />
	
	* 所有选项：insert/replace/ignore <br />

	* 默认值：insert <br />

* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与Mimer SQL的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。<br />

	* 必选：否 <br />

	* 默认值：1024 <br />

### 3.3 类型转换

目前 MimerWriter支持大部分 Mimer SQL 类型，但也存在部分没有支持的情况，请注意检查你的类型。

下面列出 MimerWriter针对 Mimer SQL类型转换列表:

| DataX 内部类型| Mimer SQL 数据类型    |
| -------- | -----  |
| Long     |bigint, integer, smallint |
| Double   |double precision, numeric, real, float, decimal |
| String   |varchar, char, clob|
| Date     |date, time, timestamp |
| Boolean  |bool|
| Bytes    |binary, varbinary, blob|
