# DataX SqlServerWriter


---


## 1 快速介绍

SqlServerWriter 插件实现了写入数据到 SqlServer 库的目的表的功能。在底层实现上， SqlServerWriter 通过 JDBC 连接远程 SqlServer 数据库，并执行相应的 insert into ...  sql 语句将数据写入 SqlServer，内部会分批次提交入库。

SqlServerWriter 面向ETL开发工程师，他们使用 SqlServerWriter 从数仓导入数据到 SqlServer。同时 SqlServerWriter 亦可以作为数据迁移工具为DBA等用户提供服务。


## 2 实现原理

SqlServerWriter 通过 DataX 框架获取 Reader 生成的协议数据，根据你配置生成相应的SQL语句


* `insert into...`(当主键/唯一性索引冲突时会写不进去冲突的行)

<br />

    注意：
    1. 目的表所在数据库必须是主库才能写入数据；整个任务至少需具备 insert into...的权限，是否需要其他权限，取决于你任务配置中在 preSql 和 postSql 中指定的语句。
    2.SqlServerWriter和MysqlWriter不同，不支持配置writeMode参数。


## 3 功能说明

### 3.1 配置样例

* 这里使用一份从内存产生到 SqlServer 导入的数据。

```
{
    "job": {
        "setting": {
            "speed": {
                "channel": 5
            }
        },
        "content": [
            {
                "reader": {},
                "writer": {
                    "name": "sqlserverwriter",
                    "parameter": {
                        "username": "root",
                        "password": "root",
                        "column": [
                            "db_id",
                            "db_type",
                            "db_ip",
                            "db_port",
                            "db_role",
                            "db_name",
                            "db_username",
                            "db_password",
                            "db_modify_time",
                            "db_modify_user",
                            "db_description",
                            "db_tddl_info"
                        ],
                        "connection": [
                            {
                                "table": [
                                    "db_info_for_writer"
                                ],
                                "jdbcUrl": "jdbc:sqlserver://[HOST_NAME]:PORT;DatabaseName=[DATABASE_NAME]"
                            }
                        ],
			"session": ["SET IDENTITY_INSERT TABLE_NAME ON"],
                        "preSql": [
                            "delete from @table where db_id = -1;"
                        ],
                        "postSql": [
                            "update @table set db_modify_time = now() where db_id = 1;"
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

               注意：1、在一个数据库上只能配置一个值。这与 SqlServerReader 支持多个备库探测不同，因为此处不支持同一个数据库存在多个主库的情况(双主导入数据情况)
                    2、jdbcUrl按照SqlServer官方规范，并可以填写连接附加参数信息。具体请参看 SqlServer官方文档或者咨询对应 DBA。


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

  * 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。如果要依次写入全部列，使用*表示, 例如: "column": ["\*"]

    		**column配置项必须指定，不能留空！**


               注意：1、我们强烈不推荐你这样配置，因为当你目的表字段个数、类型等有改动时，你的任务可能运行不正确或者失败
                    2、此处 column 不能配置任何常量值

  * 必选：是 <br />

  * 默认值：否 <br />

* **session**

  * 描述：DataX在获取 seqlserver 连接时，执行session指定的SQL语句，修改当前connection session属性<br />

  * 必选：否 <br />

  * 默认值：无 <br />

* **preSql**

  * 描述：写入数据到目的表前，会先执行这里的标准语句。如果 Sql 中有你需要操作到的表名称，请使用 `@table` 表示，这样在实际执行 Sql 语句时，会对变量按照实际表名称进行替换。<br />

  * 必选：否 <br />

  * 默认值：无 <br />

* **postSql**

  * 描述：写入数据到目的表后，会执行这里的标准语句。（原理同 preSql ） <br />

  * 必选：否 <br />

  * 默认值：无 <br />

* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与SqlServer的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。<br />

	* 必选：否 <br />

	* 默认值：1024 <br />



### 3.3 类型转换

类似 SqlServerReader ，目前 SqlServerWriter 支持大部分 SqlServer 类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出 SqlServerWriter 针对 SqlServer 类型转换列表:


| DataX 内部类型| SqlServer 数据类型    |
| -------- | -----  |
| Long     ||
| Double   ||
| String   ||
| Date     ||
| Boolean  ||
| Bytes    ||



## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征
建表语句：
```

```
单行记录类似于：
```
```
#### 4.1.2 机器参数

* 执行 DataX 的机器参数为:
    1. cpu: 24 Core Intel(R) Xeon(R) CPU E5-2430 0 @ 2.20GHz
    2. mem: 94GB
	3. net: 千兆双网卡
	4. disc: DataX 数据不落磁盘，不统计此项

* SqlServer 数据库机器参数为:
    1. cpu: 4 Core Intel(R) Xeon(R) CPU E5420  @ 2.50GHz
    2. mem: 7GB

#### 4.1.3 DataX jvm 参数

    -Xms1024m -Xmx1024m -XX:+HeapDumpOnOutOfMemoryError

#### 4.1.4 性能测试作业配置

```

```

### 4.2 测试报告

#### 4.2.1 测试报告


## 5 约束限制




## FAQ

***

**Q: SqlServerWriter 执行 postSql 语句报错，那么数据导入到目标数据库了吗?**

A: DataX 导入过程存在三块逻辑，pre 操作、导入操作、post 操作，其中任意一环报错，DataX 作业报错。由于 DataX 不能保证在同一个事务完成上述几个操作，因此有可能数据已经落入到目标端。

***

**Q: 按照上述说法，那么有部分脏数据导入数据库，如果影响到线上数据库怎么办?**

A: 目前有两种解法，第一种配置 pre 语句，该 sql 可以清理当天导入数据， DataX 每次导入时候可以把上次清理干净并导入完整数据。第二种，向临时表导入数据，完成后再 rename 到线上表。

***

**Q: 上面第二种方法可以避免对线上数据造成影响，那我具体怎样操作?**

A: 可以配置临时表导入
