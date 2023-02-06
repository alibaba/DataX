
# ClickHouseWriter 插件文档


___



## 1 快速介绍

ClickHouseWriter插件实现了向ClickHouse写入数据。在底层实现上，ClickHouseWriter通过ClickHouse JDBC driver连接ClickHouse实例，并执行相应的SQL语句将数据写入ClickHouse中。


## 2 实现原理

对于用户配置Table、Column的信息，ClickHouseWriter将其拼接为SQL语句发送到ClickHouse（**注意目前只支持insert**）。
* `insert into...`(遇到主键重复时会自动忽略当前写入数据，不做更新，作用等同于`insert ignore into`)


## 3 功能说明

### 3.1 配置样例

* 配置一个从ClickHouse导出再导入另一个ClickHouse的作业:

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
          "name": "clickhousereader",
          "parameter": {
            "username": "reade",
            "password": "1234",
            "connection": [
              {
                "jdbcUrl": [
                  "jdbc:clickhouse://host:8123/db"
                ],
                "querySql": [
                  "select * from xxtable where ds='2022-06-01' "
                ]
              }
            ],
            "fetchSize": 100
          }
        },
        "writer": {
          "name": "clickhousewriter",
          "parameter": {
            "username": "username",
            "password": "password",
            "column": [
              "col1",
              "col2",
              "col3"
            ],
            "connection": [
              {
                "jdbcUrl": "jdbc:clickhouse://<host>:<port>[/<database>]",
                "table": [
                  "table1",
                  "table2"
                ]
              }
            ],
            "preSql": [],
            "postSql": [],
            "writeMode": "insert"
          }
        }
      }
    ]
  }
}
```


### 3.2 参数说明

* **host**

    * 描述：ClickHouse连接点的域名或ip，多个node之间用逗号分隔。 <br />

    * 必选：是 <br />

    * 默认值：无 <br />

* **port**

    * 描述：ClickHouse端口。 <br />

    * 必选：是 <br />

    * 默认值：9042 <br />

* **username**

    * 描述：数据源的用户名 <br />

    * 必选：否 <br />

    * 默认值：无 <br />

* **password**

    * 描述：数据源指定用户名的密码 <br />

    * 必选：否 <br />

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

* **preSql**

  * 描述：写入数据到目的表前，会先执行这里的标准语句。如果 Sql 中有你需要操作到的表名称，请使用 `@table` 表示，这样在实际执行 SQL 语句时，会对变量按照实际表名称进行替换。比如希望导入数据前，先对表中数据进行删除操作，那么你可以这样配置：`"preSql":["truncate table @table"]`，效果是：在执行到每个表写入数据前，会先执行对应的 `truncate table 对应表名称` <br />

  * 必选：否 <br />

  * 默认值：无 <br />

* **postSql**

  * 描述：写入数据到目的表后，会执行这里的标准语句。（原理同 preSql ） <br />

  * 必选：否 <br />

  * 默认值：无 <br />

* **batchSize**

  * 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与 Adb MySQL 的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。<br />

  * 必选：否 <br />

  * 默认值：2048 <br />

* **writeMode**

  * 描述：控制写入数据到目标表采用 `insert into` 语句<br />

  * 必选：是 <br />

  * 只支持：insert <br />

  * 默认值：insert <br />


### 3.3 类型转换

目前ClickHouseWriter支持大部分ClickHouse类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出ClickHouseWriter针对ClickHouse类型转换列表:


| DataX 内部类型 | ClickHouse 数据类型                 |
|------------|---------------------------------|
| Long       | tinyint, smallint, int, bigint  |
| Double     | float, double, decimal          |
| String     | varchar                         |
| Date       | date, time, datetime, timestamp |
| Boolean    | boolean                         |
| Bytes      | binary                          |
| Map        | map                             |



## 4 性能报告

略

## 5 约束限制

### 5.1 主备同步数据恢复问题

略

## 6 FAQ



