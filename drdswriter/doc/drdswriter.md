# DataX DRDSWriter


---


## 1 快速介绍

DRDSWriter 插件实现了写入数据到 DRDS 的目的表的功能。在底层实现上， DRDSWriter 通过 JDBC 连接远程 DRDS 数据库的 Proxy，并执行相应的 replace into ... 的 sql 语句将数据写入 DRDS，特别注意执行的 Sql 语句是 replace into，为了避免数据重复写入，需要你的表具备主键或者唯一性索引(Unique Key)。

DRDSWriter 面向ETL开发工程师，他们使用 DRDSWriter 从数仓导入数据到 DRDS。同时 DRDSWriter 亦可以作为数据迁移工具为DBA等用户提供服务。


## 2 实现原理

DRDSWriter 通过 DataX 框架获取 Reader 生成的协议数据，通过 `replace into...`(没有遇到主键/唯一性索引冲突时，与 insert into 行为一致，冲突时会用新行替换原有行所有字段) 的语句写入数据到 DRDS。DRDSWriter 累积一定数据，提交给 DRDS 的 Proxy，该 Proxy 内部决定数据是写入一张还是多张表以及多张表写入时如何路由数据。
<br />

    注意：整个任务至少需要具备 replace into...的权限，是否需要其他权限，取决于你任务配置中在 preSql 和 postSql 中指定的语句。


## 3 功能说明

### 3.1 配置样例

* 这里使用一份从内存产生到 DRDS 导入的数据。

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
                    "name": "drdswriter",
                    "parameter": {
                        "writeMode": "insert",
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
                                "jdbcUrl": "jdbc:mysql://127.0.0.1:3306/datax?useUnicode=true&characterEncoding=gbk",
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

	* 描述：目的数据库的 JDBC 连接信息。作业运行时，DataX 会在你提供的 jdbcUrl 后面追加如下属性：yearIsDateType=false&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true

               注意：1、在一个数据库上只能配置一个 jdbcUrl 值
               		2、一个DRDS 写入任务仅能配置一个 jdbcUrl
                    3、jdbcUrl按照Mysql/DRDS官方规范，并可以填写连接附加控制信息，比如想指定连接编码为 gbk ，则在 jdbcUrl 后面追加属性 useUnicode=true&characterEncoding=gbk。具体请参看 Mysql/DRDS官方文档或者咨询对应 DBA。


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

	* 描述：目的表的表名称。 只能配置一个DRDS 的表名称。

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

	* 描述：写入数据到目的表前，会先执行这里的标准语句。比如你想在导入数据前清空数据表中的数据，那么可以配置为:`"preSql":["delete from yourTableName"]` <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **postSql**

	* 描述：写入数据到目的表后，会执行这里的标准语句。（原理同 preSql ） <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **writeMode**

	* 描述：默认为 replace，目前仅支持 replace，可以不配置。 <br />

	* 必选：否 <br />

	* 默认值：replace <br />

* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与DRDS的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。<br />

	* 必选：否 <br />

	* 默认值：<br />

### 3.3 类型转换

类似 MysqlWriter ，目前 DRDSWriter 支持大部分 Mysql 类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出 DRDSWriter 针对 Mysql 类型转换列表:


| DataX 内部类型| Mysql 数据类型    |
| -------- | -----  |
| Long     |int, tinyint, smallint, mediumint, int, bigint, year|
| Double   |float, double, decimal|
| String   |varchar, char, tinytext, text, mediumtext, longtext    |
| Date     |date, datetime, timestamp, time    |
| Boolean  |bit, bool   |
| Bytes    |tinyblob, mediumblob, blob, longblob, varbinary    |



## 4 性能报告


## 5 约束限制


## FAQ

***

**Q: DRDSWriter 执行 postSql 语句报错，那么数据导入到目标数据库了吗?**

A: DataX 导入过程存在三块逻辑，pre 操作、导入操作、post 操作，其中任意一环报错，DataX 作业报错。由于 DataX 不能保证在同一个事务完成上述几个操作，因此有可能数据已经落入到目标端。

***

**Q: 按照上述说法，那么有部分脏数据导入数据库，如果影响到线上数据库怎么办?**

A: 目前有两种解法，第一种配置 pre 语句，该 sql 可以清理当天导入数据， DataX 每次导入时候可以把上次清理干净并导入完整数据。第二种，向临时表导入数据，完成后再 rename 到线上表。

***

**Q: 上面第二种方法可以避免对线上数据造成影响，那我具体怎样操作?**

A: 可以配置临时表导入
