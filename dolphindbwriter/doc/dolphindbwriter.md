# DolphinDBWriter插件文档

## 1. 快速介绍

基于 DataX 的扩展功能，dolphindbwriter 插件实现了向 DolphinDB 写入数据。使用 DataX 的现有 reader 插件结合 DolphinDBWriter 插件，即可满足从不同数据源向 DolphinDB 同步数据。

## 2 实现原理

DolphinDBWriter 底层依赖 DolphinDB Java API，采用批量写入的方式将数据写入分布式数据库。

本插件通常用于以下两个场景：

1. 定期从数据源向 DolphinDB 追加新增数据。
2. 定期获取更新的数据，定位 DolphinDB 中的相同数据并进行更新。此种模式下，由于需要将历史数据读取出来并在内存中进行匹配，会需要大量的内存，因此这种场景适用于在 DolphinDB 中容量较小的表，通常建议使用数据量在 200 万以下的表。

当前使用的更新数据的模式是通过全表数据提取、更新后删除分区重写的方式来实现。**注意，目前版本还无法保障上述整体操作的原子性，后续版本会针对此种方式的事务处理方面进行优化和改进。**

## 3 功能说明

### 3.1 配置样例

* 这是一份从内存产生到DolphinDB导入的数据

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
                "value": 9,
                "type": "long"
              },
              {
                "value": 9999,
                "type": "long"
              }
            ],
            "sliceRecordCount": 1
          }
        },
        "writer": {
          "name": "dolphindbwriter",
          "parameter": {
            "userId": "admin",
            "pwd": "123456",
            "host": "172.22.240.1",
            "port": 8848,
            "dbPath": "dfs://test",
            "tableName": "employee",
            "batchSize": 1000000,
            "table": [
              {
                "type": "DT_INT",
                "name": "id"
              },
              {
                "type": "DT_INT",
                "name": "age"
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

* **host**

  * 描述：Server Host。
  * 必选：是。
  * 默认值：无。

* **port**

  * 描述：Server Port。
  * 必选：是。
  * 默认值：无。

* **username/userId**

  * 描述：DolphinDB 用户名。导入分布式库时，必须要有权限的用户才能操作，否则会返回。
  * 必选：是。
  * 默认值：无。

* **password/pwd**

  * 描述：DolphinDB 用户密码。
  * 必选：是。
  * 默认值：无。

* **dbPath**

  * 描述：需要写入的目标分布式库名称，比如 "dfs://MYDB"。
  * 必选：是。
  * 默认值：无。

* **preSql**

  * 描述：写入数据到目的表前，会先执行这里的 DolphinDB 脚本。
  * 必选：否。
  * 默认值：无。

* **postSql**

  * 描述：写入数据到目的表后，会执行这里的 DolphinDB 脚本。
  * 必选：否。
  * 默认值：无。

* **column**

  * 描述：目的表需要写入数据的字段。注意：不能与 table  字段同时使用。
  * 必选：否。
  * 默认值：无。
  * 使用说明：
    * 字段之间用英文逗号分隔，例如: "column": ["id","name","age"]。
    * 如果要依次写入全部列，使用表示，例如: "column": ["*"]。
    * 不可以为空 “column": []，或者”column": [““]。
    * 导入数据是按照原表的字段顺序，无法通过识别新表的字段名称而改变数据顺序。

* **tableName**

  * 描述: 目标数据表名称。
  * 必选: 是。
  * 默认值: 无。

* **batchSize**

  * 描述: datax 每次写入 dolphindb 的批次记录数。
  * 必须: 否。
  * 默认值: 10,000,000。

* **table**

  * 描述：写入表的字段集合。内部结构为：`{"name": "columnName", "type": "DT_STRING", "isKeyField":true}`。请注意此处列定义的顺序，需要与原表提取的列顺序完全一致。
    * name：字段名称。
    * isKeyField：是否唯一键值，可以允许组合唯一键。本属性用于数据更新场景，用于确认更新数据的主键，若无更新数据的场景，无需设置。
    * type：枚举值以及对应 DataX 数据类型如下表。
    * 必选：是。
    * 默认值：无。

* **saveFunctionName**

  * 描述：自定义数据处理函数。若未指定此配置，插件在接收到 reader 的数据后，会将数据提交到 DolphinDB 并通过 tableInsert 函数写入指定库表；如果定义此参数，则会用指定函数替换 tableInsert 函数。
  * 必选：否。
  * 默认值：无。 也可以指定自定义函数。
    插件内置了 savePartitionedData(更新分布式表)/saveDimensionData(更新维度表) 两个函数，当 saveFunctionDef 未定义或为空时，saveFunctionName 可以取枚举值之一，对应用于更新分布式表和维度表的数据处理。

* **saveFunctionDef**

  * 描述：数据入库自定义函数。此函数指 用dolphindb 脚本来实现的数据入库过程。此函数必须接受三个参数：*dfsPath*(分布式库路径)，*tbName*(数据表名)，*data*(从 datax 导入的数据，table 格式)
  * 必选：当 saveFunctionName 参数不为空且非两个枚举值之一时，此参数必填。
  * 默认值：无。

* 引入 DolphinDB 的 [`upsert!`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/u/upsert%21.html) 功能，修改配置文件 BASECODE.json 中的 `writer` 部分。

  ``` 
  "saveFunctionName":"upsertTable",
  "saveFunctionDef":"ignoreNull=true;keyColNames=`id;sortColumns=`value"
  ```

### 3.3 类型转换

DolphinDB 的数据类型及精度请参考[数据类型](https://www.dolphindb.cn/cn/help/DataTypesandStructures/DataTypes/index.html)。

| DolphinDB 类型 | 配置值          | DataX 类型 |
| :------------- | :-------------- | :--------- |
| DOUBLE         | DT_DOUBLE       | DOUBLE     |
| FLOAT          | DT_FLOAT        | DOUBLE     |
| BOOL           | DT_BOOL         | BOOLEAN    |
| DATE           | DT_DATE         | DATE       |
| MONTH          | DT_MONTH        | DATE       |
| DATETIME       | DT_DATETIME     | DATE       |
| TIME           | DT_TIME         | DATE       |
| SECOND         | DT_SECOND       | DATE       |
| TIMESTAMP      | DT_TIMESTAMP    | DATE       |
| NANOTIME       | DT_NANOTIME     | DATE       |
| NANOTIMETAMP   | DT_NANOTIMETAMP | DATE       |
| INT            | DT_INT          | LONG       |
| LONG           | DT_LONG         | LONG       |
| UUID           | DT_UUID         | STRING     |
| SHORT          | DT_SHORT        | LONG       |
| STRING         | DT_STRING       | STRING     |
| SYMBOL         | DT_SYMBOL       | STRING     |

### 3.4 增量数据导入

增量数据分两种类型，一种是新增数据，另一种是已有数据的更新，即更新了数据内容以及时间戳。对于这两种类型的数据要用不同的数据导入方式。

* **新增数据增量同步**
  新增数据的增量同步与全量导入相比，唯一的不同点在于 reader 对数据源中已导入数据的过滤。通常需要处理增量数据的表，都会有一个时间戳的列来标记入库时间，在 oralce reader 插件中，只需要配置 where 条件，增加时间戳过滤即可，其对于 dolphindbwriter 的配置与全量导入完全相同。比如时间戳字段为 OPDATE， 要增量导入 2020.03.01 之后的增量数据，则配置 `"where": "OPDATE > to_date('2020-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')`。
* **变更数据增量同步**
  * 变更数据在数据源有不同的记录方法，比较规范的方法是通过一个变更标志和时间戳来记录，比如用 OPTYPE、 OPDATE 来记录变更的类型和时间戳，这样可以通过类似 `"where": "OPTYPE=1 and OPDATE > to_date('2020-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')` 条件过滤出增量数据。
    对于 writer 的配置项，需要增加如下两处配置:
    * isKeyField
      因为变更数据的更新需要目标表中有唯一列，所以 writer 的配置中，需要对 table 配置项中唯一键列增加 `isKeyField=true` 这一配置项。
    * saveFunctionName
      DolphinDB 有多种数据存储的方式，比较常用的两种是分布式表和维度表。dolphindbwriter 中内置了更新这两种表的脚本模板，当从数据源中过滤出变更数据之后，在 writer 配置中增加 `saveFunctionName` 和 `saveFunctionDef` 两个配置(具体用法请参考附录)，writer 会根据这两个配置项，采用对应的方式将数据更新到 DolphinDB 中。  
      在 1.30.21.4 版本中，用户可通过 `saveFunctionName` 和 `saveFunctionDef` 引入 DolphinDB 的[upsert!](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/u/upsert%21.html)功能以保证导入后的数据唯一性。具体配置示例参考附录。
  * 当有些数据源中不包含 OPTYPE 这一标识列，无法分辨出新数据是更新或是新增的时候，可以作为新增数据入库，以函数视图输出的方式：
    * 数据作为新增数据处理。这种方式处理后，数据表中存在重复键值。
    * 定义 functionView 作为数据访问接口，在 functionView 中对有重复键值的数据仅输出时间戳最新的一条。
    * 用户不能直接访问表(可以取消非管理员用户访问表的权限)，统一通过 functionView 访问数据。

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

| BatchSize | 任务平均流量 | 记录写入速度 | 网卡流出流量 | 网卡进入流量 |
| --------- | ------------ | ------------ | ------------ | ------------ |
| 100000    | 10.7MB/s     | 32501rec/s   | 11.95MB/s    | 7.19Mb/s     |
| 10000     | 8.94MB/s     | 27203rec/s   | 10.7MB/s     | 6.37MB/s     |
| 1000      | 3.90MB/s     | 11851rec/s   | 4.64MB/s     | 2.78MB/s     |

> 需要注意的是，如果出现爆jvm内存情况，可以适当在DataX的core.json中修改相关配置，其默认值为1G，如果无法扩大jvm内存配置，之后再考虑降低batchsize

## FAQ

略
