# DolphindbReader插件文档

## 1 快速介绍

基于 DataX 的扩展功能，dolphindbreader 插件实现了从 DolphinDB 读出数据。使用 DataX 官方现有的 writer 插件结合 dolphindbreader 插件，即可满足从 DolphinDB 向不同数据源同步数据。

dolphindbreader 底层依赖 DolphinDB Java API，采用批量读出的方式将分布式数据库的数据读出。

**注意：如果一次性读出的 DolphinDB 源表过大，会造成插件 OOM 报错，建议使用读出数据量在 200 万以下的表。**

## 2 实现原理

简而言之，DolphinDBReader通过JDBC连接器连接到远程的DolphinDB数据库，并根据用户配置的信息生成查询SELECT SQL语句，然后发送到远程DolphinDB数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column、Where的信息，DolphinDBReader将其拼接为SQL语句发送到DolphinDB数据库；对于用户配置querySql信息，DolphinDBReader直接将其发送到DolphinDB数据库。

## 3 功能说明

### 3.1 配置样例

* 配置一个从DolphinDB数据库同步抽取数据到本地的作业

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 1
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0.02
      }
    },
    "content": [
      {
        "reader": {
          "name": "dolphindbreader",
          "parameter": {
            "columns": [
              {
                "name": "id"
              },
              {
                "name": "age"
              }
            ],
            "userId": "admin",
            "pwd": "123456",
            "host": "172.22.240.1",
            "port": 8848,
            "dbPath": "dfs://test",
            "tableName": "employee",
            "where": ""
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print":true
          }
        }
      }
    ]
  }
}

```

* 配置一个自定义SQL的数据库同步任务到本地内容的作业

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 1
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0.02
      }
    },
    "content": [
      {
        "reader": {
          "name": "dolphindbreader",
          "parameter": {
            "userId": "admin",
            "pwd": "123456",
            "host": "172.22.240.1",
            "port": 8848,
            "dbPath": "dfs://test",
            "tableName": "employee",
            "querySql": "select id, age from loadTable(\"dfs://test\", `employee)"
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print":true
          }
        }
      }
    ]
  }
}
```

### 3.2 参数说明

- host
  - 描述：Server Host。
  - 必选：是。
  - 默认值：无。
- port
  - 描述：Server Port。
  - 必选：是。
  - 默认值：无。
- userId
  - 描述：DolphinDB 用户名。导出分布式库时，必须要有权限的用户才能操作，否则会返回。
  - 必选：是。
  - 默认值：无。
- pwd
  - 描述：DolphinDB 用户密码。
  - 必选：是。
  - 默认值：无。
- dbPath
  - 描述：需要读出的目标分布式库名称，比如 `dfs://MYDB`。
  - 必选：是。
  - 默认值：无
- tableName
  - 描述: 读出数据表名称。
  - 必选: 是。
  - 默认值: 无。
- where
  - 描述: 可以通过指定 where 来设置条件，比如 "id >10 and name = `dolphindb"。
  - 必选: 是。
  - 默认值: 无。
-  columns
   - 描述：读出表的字段集合。内部结构为：`{"name": "columnName"}`。请注意此处列定义的顺序，需要与原表提取的列顺序完全一致。
   - 必选: 是。
   - 默认值: 无。
- name：
  - 描述：字段名称。
  - 必选: 是。
  - 默认值: 无。
- querySql:
  - 描述：在部分业务场景下，若配置项参数 where 无法描述筛选条件，用户可使用 querySql 以实现 SQL 自定义筛选。 注意，若用户配置了 querySql，则插件 dolphindbreader 将忽略配置项参数 table, where 的筛选条件，即 querySql 的优先级大于table, where。
  - 必选：否。
  - 默认值：无。
- splitPk:
  - 描述：分布式数据库表的分区键，用于分区表的数据读取。若不配置，则默认读取所有分区的数据。
  - 必选：否。
  - 默认值：无。
### 3.3 类型转换

下表为数据对照表（其他数据类型暂不支持）

| DolphinDB类型  | 配置值             | DataX类型 |
| ------------ | --------------- | ------- |
| DOUBLE       | DT_DOUBLE       | DOUBLE  |
| FLOAT        | DT_FLOAT        | DOUBLE  |
| BOOL         | DT_BOOL         | BOOLEAN |
| DATE         | DT_DATE         | DATE    |
| DATETIME     | DT_DATETIME     | DATE    |
| TIME         | DT_TIME         | STRING  |
| TIMESTAMP    | DT_TIMESTAMP    | DATE    |
| NANOTIME     | DT_NANOTIME     | STRING  |
| NANOTIMETAMP | DT_NANOTIMETAMP | DATE    |
| MONTH        | DT_MONTH        | DATE    |
| BYTE         | DT_BYTE         | LONG    |
| LONG         | DT_LONG         | LONG    |
| SHORT        | DT_SHORT        | LONG    |
| INT          | DT_INT          | LONG    |
| UUID         | DT_UUID         | STRING  |
| STRING       | DT_STRING       | STRING  |
| BLOB         | DT_BLOB         | STRING  |
| SYMBOL       | DT_SYMBOL       | STRING  |
| COMPLEX      | DT_COMPLEX      | STRING  |
| DATEHOUR     | DT_DATEHOUR     | DATE    |
| DURATION     | DT_DURATION     | LONG    |
| INT128       | DT_INT128       | STRING  |
| IPADDR       | DT_IPADDR       | STRING  |
| MINUTE       | DT_MINUTE       | STRING  |
| MONTH        | DT_MONTH        | STRING  |
| POINT        | DT_POINT        | STRING  |
| SECOND       | DT_SECOND       | STRING  |

## 4 约束限制

略

## 5 性能报告

### 5.1 环境准备

#### 5.1.1 数据特征

建表语句：

```
model = table(1:0, `SecurityID`DateTime`PreClosePx`OpenPx`HighPx`LowPx`LastPx`Volume`Amount`BidPrice1`BidPrice2`BidPrice3`BidPrice4`BidPrice5`BidOrderQty1`BidOrderQty2`BidOrderQty3`BidOrderQty4`BidOrderQty5`OfferPrice1`OfferPrice2`OfferPrice3`OfferPrice4`OfferPrice5`OfferQty1`OfferQty2`OfferQty3`OfferQty4`OfferQty5, [SYMBOL, DATETIME, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, LONG, LONG, LONG, LONG, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, LONG, LONG, LONG, LONG])

// OLAP 存储引擎建库建表
dbDate = database("", VALUE, 2020.06.01..2020.06.07)
dbSecurityID = database("", HASH, [SYMBOL, 10])
drop database if exists "dfs://Level1"
db = database("dfs://Level1", COMPO, [dbDate, dbSecurityID])
createPartitionedTable(db, model, `Snapshot, `DateTime`SecurityID)
```

#### 5.1.2 机器参数

* 执行DataX的机器参数为：
  * CPU: 48核 Intel(R) Xeon(R) Silver 4214 CPU @ 2.20GHz
  * MEM：502G
  * NET：千兆网卡

### 5.2 测试报告

| 指标         | 数值 |
| ------------ | ---------- |
| 任务平均流量 | 5.80MB/S   |
| 记录写入速度 | 16000rec/S |
| 网卡流出流量 | 2.16MB/s   |
| 网卡进入流量 | 4.24MB/s   |

## FAQ

略
