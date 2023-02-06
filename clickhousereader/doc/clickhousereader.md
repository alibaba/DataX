
# ClickHouseReader 插件文档


___



## 1 快速介绍

ClickHouseReader插件实现了读取ClickHouse数据。在底层实现上，ClickHouseReader通过ClickHouse JDBC driver连接ClickHouse实例，并执行相应的select SQL语句读取ClickHouse中的数据。


## 2 实现原理

对于用户配置Table、Column的信息，ClickHouseReader将其拼接为SQL语句发送到ClickHouse。
* `select ...`


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

* **fetchSize**

	* 描述：该配置项定义了插件和数据库服务器端每次批量数据获取条数，该值决定了DataX和服务器端的网络交互次数，能够较大的提升数据抽取性能。<br />

  `注意，该值过大(>2048)可能造成DataX进程OOM。`。

	* 必选：否 <br />

	* 默认值：1024 <br />

* **querySql**

	* 描述：在有些业务场景下，where这一配置项不足以描述所筛选的条件，用户可以通过该配置型来自定义筛选SQL。当用户配置了这一项之后，DataX系统就会忽略table，column这些配置型，直接使用这个配置项的内容对数据进行筛选，例如需要进行多表join后同步数据，使用select a,b from table_a join table_b on table_a.id = table_b.id <br />

  `当用户配置querySql时，MysqlReader直接忽略table、column、where条件的配置`，querySql优先级大于table、column、where选项。

	* 必选：否 <br />

	* 默认值：无 <br />


### 3.3 类型转换

目前ClickHouseReader支持大部分ClickHouse类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出ClickHouseReader针对ClickHouse类型转换列表:


| DataX 内部类型 | ClickHouse 数据类型                 |
|------------|---------------------------------|
| Long       | tinyint, smallint, int, bigint  |
| Double     | float, double, decimal          |
| String     | varchar                         |
| Date       | date, time, datetime, timestamp |
| Boolean    | boolean                         |
| Bytes      | binary                          |



## 4 性能报告

略

## 5 约束限制

### 5.1 主备同步数据恢复问题

略

## 6 FAQ



