# DataX TDengineReader

## 1 快速介绍

TDengineReader 插件实现了 TDengine 读取数据的功能。

## 2 实现原理

TDengineReader 通过 TDengine 的 JDBC driver 查询获取数据。

## 3 功能说明

### 3.1 配置样例

* 配置一个从 TDengine 抽取数据作业:

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "tdenginereader",
          "parameter": {
            "username": "root",
            "password": "taosdata",
            "connection": [
              {
                "table": [
                  "meters"
                ],
                "jdbcUrl": [
                  "jdbc:TAOS-RS://192.168.56.105:6041/test?timestampFormat=TIMESTAMP"
                ]
              }
            ],
            "column": [
              "ts",
              "current",
              "voltage",
              "phase"
            ],
            "where": "ts>=0",
            "beginDateTime": "2017-07-14 10:40:00",
            "endDateTime": "2017-08-14 10:40:00"
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "encoding": "UTF-8",
            "print": true
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 1
      }
    }
  }
}
```

* 配置一个自定义 SQL 的数据抽取作业：

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "tdenginereader",
          "parameter": {
            "user": "root",
            "password": "taosdata",
            "connection": [
              {
                "querySql": [
                  "select * from test.meters"
                ],
                "jdbcUrl": [
                  "jdbc:TAOS-RS://192.168.56.105:6041/test?timestampFormat=TIMESTAMP"
                ]
              }
            ]
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "encoding": "UTF-8",
            "print": true
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 1
      }
    }
  }
}
```

### 3.2 参数说明

* **username**
    * 描述：TDengine 实例的用户名 <br />
    * 必选：是 <br />
    * 默认值：无 <br />
* **password**
    * 描述：TDengine 实例的密码 <br />
    * 必选：是 <br />
    * 默认值：无 <br />
* **jdbcUrl**
    * 描述：TDengine 数据库的JDBC连接信息。注意，jdbcUrl必须包含在connection配置单元中。JdbcUrl具体请参看TDengine官方文档。
    * 必选：是 <br />
    * 默认值：无<br />
* **querySql**
    * 描述：在有些业务场景下，where 这一配置项不足以描述所筛选的条件，用户可以通过该配置型来自定义筛选SQL。当用户配置了 querySql 后， TDengineReader 就会忽略 table, column,
      where, beginDateTime, endDateTime这些配置型，直接使用这个配置项的内容对数据进行筛选。例如需要 进行多表join后同步数据，使用 select a,b from table_a join
      table_b on table_a.id = table_b.id<br />
    * 必选：否 <br />
    * 默认值：无 <br />
* **table**
    * 描述：所选取的需要同步的表。使用 JSON 的数组描述，因此支持多张表同时抽取。当配置为多张表时，用户自己需保证多张表是同一 schema 结构， TDengineReader不予检查表是否同一逻辑表。注意，table必须包含在
      connection 配置单元中。<br />
    * 必选：是  <br />
    * 默认值：无 <br />
* **where**
    * 描述：筛选条件中的 where 子句，TDengineReader 根据指定的column, table, where, begingDateTime, endDateTime 条件拼接 SQL，并根据这个 SQL
      进行数据抽取。 <br />
    * 必选：否 <br />
    * 默认值：无 <br />
* **beginDateTime**
    * 描述：数据的开始时间，Job 迁移从 begineDateTime 到 endDateTime 的数据，格式为 yyyy-MM-dd HH:mm:ss <br />
    * 必选：否 <br />
    * 默认值：无 <br />
* **endDateTime**
    * 描述：数据的结束时间，Job 迁移从 begineDateTime 到 endDateTime 的数据，格式为 yyyy-MM-dd HH:mm:ss <br />
    * 必选：否 <br />
    * 默认值：无 <br />

### 3.3 类型转换

| TDengine 数据类型 | DataX 内部类型 |
| --------------- | ------------- |
| TINYINT         | Long          |
| SMALLINT        | Long          |
| INTEGER         | Long          |
| BIGINT          | Long          |
| FLOAT           | Double        |
| DOUBLE          | Double        |
| BOOLEAN         | Bool          |
| TIMESTAMP       | Date          |
| BINARY          | Bytes         |
| NCHAR           | String        |

## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征

#### 4.1.2 机器参数

#### 4.1.3 DataX jvm 参数

	-Xms1024m -Xmx1024m -XX:+HeapDumpOnOutOfMemoryError

### 4.2 测试报告

#### 4.2.1 单表测试报告

| 通道数| DataX速度(Rec/s)|DataX流量(MB/s)| DataX机器网卡流出流量(MB/s)|DataX机器运行负载|DB网卡进入流量(MB/s)|DB运行负载|DB TPS|
|--------| --------|--------|--------|--------|--------|--------|--------|
|1|                  |                 |                             |                   |                      |            |        |
|4|                  |                 |                             |                   |                      |            |        |
|8|                  |                 |                             |                   |                      |            |        |
|16|                  |                 |                             |                   |                      |            |        |
|32|                  |                 |                             |                   |                      |            |        |

说明：

#### 4.2.4 性能测试小结

1.
2.

## 5 约束限制

## FAQ