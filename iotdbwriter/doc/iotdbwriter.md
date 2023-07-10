# IoTDBWriter 插件文档

___

## 1 快速介绍

IoTDBWriter 插件实现了向 IoTDB 写入数据。在底层实现上，IoTDBWriter 通过 IoTDB.SessionPool 连接远程 IoTDB 数据库，并通过 SessionPool.insertTable 向 IoTDB 写入数据。


## 2 实现原理

IoTDBWriter 的实现原理如下：

1. 通过 IoTDB.SessionPool 连接用户配置的远程 IoTDB 数据库。
2. 每个子任务维护一个 IoTDB.Tablet，每收到上游 Reader 传来的 record，就在 Tablet 缓存攒批。
3. 每当 Tablet 缓存的数据量达到用户配置的 batchSize 时，就调用 IoTDB.SessionPool.insertTable 向 IoTDB 写入数据。

## 3 功能说明

### 3.1 配置示例

* 配置一个从 MySQL 到 IoTDB 导入的作业:
```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "username": "root",
            "password": "root",
            "column": ["timestamp", "s_0", "s_1", "s_2", "s_3", "s_4", "s_5"],
            "connection": [
              {
                "jdbcUrl": ["jdbc:mysql://127.0.0.1:3306/root"],
                "table": [
                  "`test.g_0.d_0`",
                  "`test.g_0.d_2`",
                  "`test.g_0.d_4`",
                  "`test.g_0.d_6`",
                  "`test.g_0.d_8`",
                  "`test.g_1.d_1`",
                  "`test.g_1.d_3`",
                  "`test.g_1.d_5`",
                  "`test.g_1.d_7`",
                  "`test.g_1.d_9`"
                ]
              }
            ]
          }
        },
        "writer": {
          "name": "iotdbwriter",
          "parameter": {
            "addresses": "127.0.0.1:6667",
            "username": "root",
            "password": "root",
            "sessionPoolMaxSize": 3,
            "batchSize": 32768,
            "databases": {
              "root.test.g_0": {
                "d_0": {
                  "sensors": ["s_0", "s_1", "s_2", "s_3", "s_4", "s_5"],
                  "dataTypes": ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT"]
                },
                "d_2": {
                  "sensors": ["s_0", "s_1", "s_2", "s_3", "s_4", "s_5"],
                  "dataTypes": ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT"]
                },
                "d_4": {
                  "sensors": ["s_0", "s_1", "s_2", "s_3", "s_4", "s_5"],
                  "dataTypes": ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT"]
                },
                "d_6": {
                  "sensors": ["s_0", "s_1", "s_2", "s_3", "s_4", "s_5"],
                  "dataTypes": ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT"]
                },
                "d_8": {
                  "sensors": ["s_0", "s_1", "s_2", "s_3", "s_4", "s_5"],
                  "dataTypes": ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT"]
                }
              },
              "root.test.g_1": {
                "d_1": {
                  "sensors": ["s_0", "s_1", "s_2", "s_3", "s_4", "s_5"],
                  "dataTypes": ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT"]
                },
                "d_3": {
                  "sensors": ["s_0", "s_1", "s_2", "s_3", "s_4", "s_5"],
                  "dataTypes": ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT"]
                },
                "d_5": {
                  "sensors": ["s_0", "s_1", "s_2", "s_3", "s_4", "s_5"],
                  "dataTypes": ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT"]
                },
                "d_7": {
                  "sensors": ["s_0", "s_1", "s_2", "s_3", "s_4", "s_5"],
                  "dataTypes": ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT"]
                },
                "d_9": {
                  "sensors": ["s_0", "s_1", "s_2", "s_3", "s_4", "s_5"],
                  "dataTypes": ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT"]
                }
              }
            }
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 10
      }
    }
  }
}
```

### 3.2 参数说明

+ **address**
    + 描述：被连接的远程 IoTDB 地址，可同时配置分布式 IoTDB 的多个 DataNode，保证数据同步任务不会因单个 DataNode 宕机而失败，多个地址之间用英文逗号分隔。
    + 必选项：是
    + 默认值："127.0.0.1:6667"
+ **username**
    + 描述：IoTDB 的用户名
    + 必选项：是
    + 默认值："root"
+ **password**
    + 描述：IoTDB 的密码
    + 必选项：是
    + 默认值："root"
+ **batchSize**
    + 描述：IoTDBWriter 一次向 IoTDB 写入的数据量，单位为 record 数量
    + 必选项：是
    + 默认值：32768
+ **sessionPoolMaxSize**
    + 描述：IoTDB.SessionPool 的最大连接数
    + 必选项：否
    + 默认值：3
+ **databases**
    + 描述：需要同步的 IoTDB 库，支持同步多个库，每个库的配置项如下：
        + database
            + 描述：需要同步的 IoTDB 库名
            + 必选项：是
            + 默认值：无
        + device
            + 描述：需要同步的 IoTDB 设备名
            + 必选项：是
            + 默认值：无
            + **请注意**：请配置 "channel" 等于 device 数量，每个任务负责同步一个 device 的数据。
        + dataTypes
            + 描述：需要同步的 IoTDB 设备的数据类型，支持的数据类型有：BOOLEAN、INT32、INT64、FLOAT、DOUBLE、TEXT
            + 必选项：是
            + 默认值：无
        + sensors
            + 描述：需要同步的 IoTDB 设备的传感器名
            + 必选项：是
            + 默认值：无
            + **请注意**：IoTDBWriter 会默认从第0列读取 timestamp，请不要在 sensor 配置时间戳列。
    + 必选项：是
    + 默认值：无

## 4 性能报告

### 4.1 测试环境

+ 系统：macOS
+ CPU：Apple M1 Pro
+ 内存：16 GB

### 4.2 任务配置

同步 MySQL 的两个 Database 至 IoTDB 中，每个 Database 同步 5 个 Table，每个 Table 有 6 个 column，每个 column 同步 2592000 条数据（每秒一个数据点，一个月的数据）。

### 4.3 测试结果

```
2023-05-30 16:41:19.466 [job-0] INFO  JobContainer -
	 [total cpu info] =>
		averageCpu                     | maxDeltaCpu                    | minDeltaCpu
		-1.00%                         | -1.00%                         | -1.00%


	 [total gc info] =>
		 NAME                 | totalGCCount       | maxDeltaGCCount    | minDeltaGCCount    | totalGCTime        | maxDeltaGCTime     | minDeltaGCTime
		 PS MarkSweep         | 2                  | 2                  | 2                  | 0.056s             | 0.056s             | 0.056s
		 PS Scavenge          | 172                | 172                | 172                | 3.292s             | 3.292s             | 3.292s

2023-05-30 16:41:19.466 [job-0] INFO  JobContainer - PerfTrace not enable!
2023-05-30 16:41:19.466 [job-0] INFO  StandAloneJobContainerCommunicator - Total 25920000 records, 570240000 bytes | Speed 27.19MB/s, 1296000 records/s | Error 0 records, 0 bytes |  All Task WaitWriterTime 62.854s |  All Task WaitReaderTime 110.746s | Percentage 100.00%
2023-05-30 16:41:19.467 [job-0] INFO  JobContainer -
任务启动时刻                    : 2023-05-30 16:40:58
任务结束时刻                    : 2023-05-30 16:41:19
任务总计耗时                    :                 20s
任务平均流量                    :           27.19MB/s
记录写入速度                    :        1296000rec/s
读出记录总数                    :            25920000
读写失败总数                    :                   0
```