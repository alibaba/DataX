# IoTDBReader 插件文档

___

## 1 快速介绍

IoTDBReader 插件实现了从 IoTDB 读取数据。在底层实现上，IoTDBReader 通过 IoTDB.SessionPool 连接远程 IoTDB 数据库，并执行相应的 SQL 语句将数据从 IoTDB 库中 SELECT 出来。


## 2 实现原理

IoTDBReader 的实现原理如下：

1. 通过 IoTDB.SessionPool 连接用户配置的远程 IoTDB 数据库。
2. 对于每个用户配置的 Device，IoTDBReader 会执行 SQL `select * from <DeviceId> align by device` 将该 Device 的全部数据 SELECT 出来。
3. 对于每条 SELECT 出的记录，IoTDBReader 会根据用户定义将其转换为 DataX 的 Record 结构，然后将其传递给 DataX 的 Writer 进行处理。

**SQL `select * from <DeviceId> align by device` 的原始结果如下所示：**

```
+-----------------------------+-----------------+---+----+---+---+---+---+
|                         Time|           Device|s_1| s_0|s_3|s_2|s_5|s_4|
+-----------------------------+-----------------+---+----+---+---+---+---+
|2023-01-01T00:16:41.000+08:00|root.test.g_1.d_3|  5|true|0.0|781| HE|0.0|
|2023-01-01T00:16:42.000+08:00|root.test.g_1.d_3|  5|true|0.0|781| nT|0.0|
|2023-01-01T00:16:43.000+08:00|root.test.g_1.d_3|  5|true|0.0|781| dp|0.0|
|2023-01-01T00:16:44.000+08:00|root.test.g_1.d_3|  5|true|0.0|781| 39|0.0|
|2023-01-01T00:16:45.000+08:00|root.test.g_1.d_3|  5|true|0.0|781| vF|0.0|
+-----------------------------+-----------------+---+----+---+---+---+---+
```

## 3 功能说明

### 3.1 配置样例

* 配置一个从 IoTDB 同步到 MySQL 的作业：

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 10
      }
    },
    "content": [
      {
        "reader": {
          "name": "iotdbreader",
          "parameter": {
            "addresses": "127.0.0.1:6667",
            "username": "root",
            "password": "root",
            "sessionPoolMaxSize": 3,
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
        },
        "writer": {
          "name": "mysqlwriter",
          "parameter": {
            "username": "root",
            "password": "root",
            "writeMode": "insert",
            "column": ["timestamp", "s_0", "s_1", "s_2", "s_3", "s_4", "s_5"],
            "session": ["set session sql_mode='ANSI'"],
            "preSql": [
              "delete from @table"
            ],
            "connection": [
              {
                "jdbcUrl": "jdbc:mysql://127.0.0.1:3306/root",
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
        }
      }
    ]
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
      + **请注意**：IoTDB 生成的 DataX record 会默认将第0列设置为 timestamp，请不要在 sensor 配置时间戳列。
  + 必选项：是
  + 默认值：无

## 4 性能报告

### 4.1 测试环境

+ 系统：macOS
+ CPU：Apple M1 Pro
+ 内存：16 GB

### 4.2 任务配置

同步 IoTDB 的两个 Database 至 MySQL 中，每个 Database 同步 5 个 Device，每个 Device 同步 6 个 Sensor，每个 Sensor 同步 2592000 条数据（每秒一个数据点，一个月的数据）。

### 4.3 测试结果
```
2023-05-30 15:59:44.699 [job-0] INFO  JobContainer -
	 [total cpu info] =>
		averageCpu                     | maxDeltaCpu                    | minDeltaCpu
		-1.00%                         | -1.00%                         | -1.00%


	 [total gc info] =>
		 NAME                 | totalGCCount       | maxDeltaGCCount    | minDeltaGCCount    | totalGCTime        | maxDeltaGCTime     | minDeltaGCTime
		 PS MarkSweep         | 1                  | 1                  | 1                  | 0.014s             | 0.014s             | 0.014s
		 PS Scavenge          | 487                | 487                | 487                | 4.288s             | 4.288s             | 4.288s

2023-05-30 15:59:44.699 [job-0] INFO  JobContainer - PerfTrace not enable!
2023-05-30 15:59:44.699 [job-0] INFO  StandAloneJobContainerCommunicator - Total 25920000 records, 855360000 bytes | Speed 13.60MB/s, 432000 records/s | Error 0 records, 0 bytes |  All Task WaitWriterTime 461.269s |  All Task WaitReaderTime 53.185s | Percentage 100.00%
2023-05-30 15:59:44.701 [job-0] INFO  JobContainer -
任务启动时刻                    : 2023-05-30 15:58:44
任务结束时刻                    : 2023-05-30 15:59:44
任务总计耗时                    :                 60s
任务平均流量                    :           13.60MB/s
记录写入速度                    :         432000rec/s
读出记录总数                    :            25920000
读写失败总数                    :                   0
```