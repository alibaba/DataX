
# TSDBReader 插件文档

___


## 1 快速介绍

TSDBReader 插件实现了从阿里云 TSDB 读取数据。阿里云时间序列数据库 ( **T**ime **S**eries **D**ata**b**ase , 简称 TSDB) 是一种集时序数据高效读写，压缩存储，实时计算能力为一体的数据库服务，可广泛应用于物联网和互联网领域，实现对设备及业务服务的实时监控，实时预测告警。详见 TSDB 的阿里云[官网](https://cn.aliyun.com/product/hitsdb)。



## 2 实现原理

在底层实现上，TSDBReader 通过 HTTP 请求链接到 阿里云 TSDB 实例，利用 `/api/query` 或者 `/api/mquery` 接口将数据点扫描出来（更多细节详见：[时序数据库 TSDB  - HTTP API 概览](https://help.aliyun.com/document_detail/63557.html)）。而整个同步的过程，是通过时间线和查询时间线范围进行切分。



## 3 功能说明

### 3.1 配置样例

* 配置一个从 阿里云 TSDB 数据库同步抽取数据到本地的作业，并以**时序数据**的格式输出：

时序数据样例：
```json
{"metric":"m","tags":{"app":"a19","cluster":"c5","group":"g10","ip":"i999","zone":"z1"},"timestamp":1546272263,"value":1}
```

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "tsdbreader",
          "parameter": {
            "sinkDbType": "TSDB",
            "endpoint": "http://localhost:8242",
            "column": [
              "m"
            ],
            "splitIntervalMs": 60000,
            "beginDateTime": "2019-01-01 00:00:00",
            "endDateTime": "2019-01-01 01:00:00"
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
        "channel": 3
      }
    }
  }
}
```

* 配置一个从 阿里云 TSDB 数据库同步抽取数据到本地的作业，并以**关系型数据**的格式输出：

关系型数据样例：
```txt
m	1546272125	a1	c1	g2	i3021	z4	1.0
```

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "tsdbreader",
          "parameter": {
            "sinkDbType": "RDB",
            "endpoint": "http://localhost:8242",
            "column": [
              "__metric__",
              "__ts__",
              "app",
              "cluster",
              "group",
              "ip",
              "zone",
              "__value__"
            ],
            "metric": [
              "m"
            ],
            "splitIntervalMs": 60000,
            "beginDateTime": "2019-01-01 00:00:00",
            "endDateTime": "2019-01-01 01:00:00"
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
        "channel": 3
      }
    }
  }
}
```

* 配置一个从 阿里云 TSDB 数据库同步抽取**单值**数据到 ADB 的作业：

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "tsdbreader",
          "parameter": {
            "sinkDbType": "RDB",
            "endpoint": "http://localhost:8242",
            "column": [
              "__metric__",
              "__ts__",
              "app",
              "cluster",
              "group",
              "ip",
              "zone",
              "__value__"
            ],
            "metric": [
              "m"
            ],
            "splitIntervalMs": 60000,
            "beginDateTime": "2019-01-01 00:00:00",
            "endDateTime": "2019-01-01 01:00:00"
          }
        },
        "writer": {
          "name": "adswriter",
          "parameter": {
            "username": "******",
            "password": "******",
            "column": [
              "`metric`",
              "`ts`",
              "`app`",
              "`cluster`",
              "`group`",
              "`ip`",
              "`zone`",
              "`value`"
            ],
            "url": "http://localhost:3306",
            "schema": "datax_test",
            "table": "datax_test",
            "writeMode": "insert",
            "opIndex": "0",
            "batchSize": "2"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 3
      }
    }
  }
}
```

* 配置一个从 阿里云 TSDB 数据库同步抽取**多值**数据到 ADB 的作业：

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "tsdbreader",
          "parameter": {
            "sinkDbType": "RDB",
            "endpoint": "http://localhost:8242",
            "column": [
              "__metric__",
              "__ts__",
              "app",
              "cluster",
              "group",
              "ip",
              "zone",
              "load",
              "memory",
              "cpu"
            ],
            "metric": [
              "m_field"
            ],
            "field": {
              "m_field": [
                "load",
                "memory",
                "cpu"
              ]
            },
            "splitIntervalMs": 60000,
            "beginDateTime": "2019-01-01 00:00:00",
            "endDateTime": "2019-01-01 01:00:00"
          }
        },
        "writer": {
          "name": "adswriter",
          "parameter": {
            "username": "******",
            "password": "******",
            "column": [
              "`metric`",
              "`ts`",
              "`app`",
              "`cluster`",
              "`group`",
              "`ip`",
              "`zone`",
              "`load`",
              "`memory`",
              "`cpu`"
            ],
            "url": "http://localhost:3306",
            "schema": "datax_test",
            "table": "datax_test_multi_field",
            "writeMode": "insert",
            "opIndex": "0",
            "batchSize": "2"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 3
      }
    }
  }
}
```

* 配置一个从 阿里云 TSDB 数据库同步抽取**单值**数据到 ADB 的作业，并指定过滤部分时间线：

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "tsdbreader",
          "parameter": {
            "sinkDbType": "RDB",
            "endpoint": "http://localhost:8242",
            "column": [
              "__metric__",
              "__ts__",
              "app",
              "cluster",
              "group",
              "ip",
              "zone",
              "__value__"
            ],
            "metric": [
              "m"
            ],
            "tag": {
              "m": {
                "app": "a1",
                "cluster": "c1"
              }
            },
            "splitIntervalMs": 60000,
            "beginDateTime": "2019-01-01 00:00:00",
            "endDateTime": "2019-01-01 01:00:00"
          }
        },
        "writer": {
          "name": "adswriter",
          "parameter": {
            "username": "******",
            "password": "******",
            "column": [
              "`metric`",
              "`ts`",
              "`app`",
              "`cluster`",
              "`group`",
              "`ip`",
              "`zone`",
              "`value`"
            ],
            "url": "http://localhost:3306",
            "schema": "datax_test",
            "table": "datax_test",
            "writeMode": "insert",
            "opIndex": "0",
            "batchSize": "2"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 3
      }
    }
  }
}
```

* 配置一个从 阿里云 TSDB 数据库同步抽取**多值**数据到 ADB 的作业，并指定过滤部分时间线：

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "tsdbreader",
          "parameter": {
            "sinkDbType": "RDB",
            "endpoint": "http://localhost:8242",
            "column": [
              "__metric__",
              "__ts__",
              "app",
              "cluster",
              "group",
              "ip",
              "zone",
              "load",
              "memory",
              "cpu"
            ],
            "metric": [
              "m_field"
            ],
            "field": {
              "m_field": [
                "load",
                "memory",
                "cpu"
              ]
            },
            "tag": {
              "m_field": {
                "ip": "i999"
              }
            },
            "splitIntervalMs": 60000,
            "beginDateTime": "2019-01-01 00:00:00",
            "endDateTime": "2019-01-01 01:00:00"
          }
        },
        "writer": {
          "name": "adswriter",
          "parameter": {
            "username": "******",
            "password": "******",
            "column": [
              "`metric`",
              "`ts`",
              "`app`",
              "`cluster`",
              "`group`",
              "`ip`",
              "`zone`",
              "`load`",
              "`memory`",
              "`cpu`"
            ],
            "url": "http://localhost:3306",
            "schema": "datax_test",
            "table": "datax_test_multi_field",
            "writeMode": "insert",
            "opIndex": "0",
            "batchSize": "2"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 3
      }
    }
  }
}
```

* 配置一个从 阿里云 TSDB 数据库同步抽取**单值**数据到另一个 阿里云 TSDB 数据库 的作业：

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "tsdbreader",
          "parameter": {
            "sinkDbType": "TSDB",
            "endpoint": "http://localhost:8242",
            "column": [
              "m"
            ],
            "splitIntervalMs": 60000,
            "beginDateTime": "2019-01-01 00:00:00",
            "endDateTime": "2019-01-01 01:00:00"
          }
        },
        "writer": {
          "name": "tsdbwriter",
          "parameter": {
            "endpoint": "http://localhost:8240"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 3
      }
    }
  }
}
```

* 配置一个从 阿里云 TSDB 数据库同步抽取**多值**数据到另一个 阿里云 TSDB 数据库 的作业：

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "tsdbreader",
          "parameter": {
            "sinkDbType": "TSDB",
            "endpoint": "http://localhost:8242",
            "column": [
              "m_field"
            ],
            "field": {
              "m_field": [
                "load",
                "memory",
                "cpu"
              ]
            },
            "splitIntervalMs": 60000,
            "beginDateTime": "2019-01-01 00:00:00",
            "endDateTime": "2019-01-01 01:00:00"
          }
        },
        "writer": {
          "name": "tsdbwriter",
          "parameter": {
            "multiField": true,
            "endpoint": "http://localhost:8240"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 3
      }
    }
  }
}
```






### 3.2 参数说明

* **name**
  * 描述：本插件的名称
  * 必选：是
  * 默认值：tsdbreader

* **parameter**
  * **sinkDbType**
    * 描述：目标数据库的类型
    * 必选：否
    * 默认值：TSDB
    * 注意：目前支持 TSDB 和 RDB 两个取值。其中，TSDB 包括 阿里云 TSDB、OpenTSDB、InfluxDB、Prometheus 和 TimeScale。RDB 包括 ADB、MySQL、Oracle、PostgreSQL 和 DRDS 等。

  * **endpoint**
    * 描述：阿里云 TSDB 的 HTTP 连接地址
    * 必选：是
    * 格式：http://IP:Port
    * 默认值：无

  * **column**
    * 描述：TSDB 场景下：数据迁移任务需要迁移的 Metric 列表；RDB 场景下：映射到关系型数据库中的表字段，且增加 `__metric__`、`__ts__` 和 `__value__` 三个字段，其中 `__metric__` 用于映射度量字段，`__ts__` 用于映射 timestamp 字段，而 `__value__` 仅适用于单值场景，用于映射度量值，多值场景下，直接指定 field 字段即可
    * 必选：是
    * 默认值：无

  * **metric**
    * 描述：仅适用于 RDB 场景下，表示数据迁移任务需要迁移的 Metric 列表
    * 必选：否
    * 默认值：无

  * **field**
    * 描述：仅适用于多值场景下，表示数据迁移任务需要迁移的 Field 列表
    * 必选：否
    * 默认值：无

  * **tag**
    * 描述：数据迁移任务需要迁移的 TagK 和 TagV，用于进一步过滤时间线
    * 必选：否
    * 默认值：无

  * **splitIntervalMs**
    * 描述：用于 DataX 内部切分 Task，每个 Task 只查询一小部分的时间段
    * 必选：是
    * 默认值：无
    * 注意：单位是 ms 毫秒


* **beginDateTime**
  * 描述：和 endDateTime 配合使用，用于指定哪个时间段内的数据点，需要被迁移
  * 必选：是
  * 格式：`yyyy-MM-dd HH:mm:ss`
  * 默认值：无
  * 注意：指定起止时间会自动忽略分钟和秒，转为整点时刻，例如 2019-4-18 的 [3:35, 4:55) 会被转为 [3:00, 4:00)

* **endDateTime**
  * 描述：和 beginDateTime 配合使用，用于指定哪个时间段内的数据点，需要被迁移
  * 必选：是
  * 格式：`yyyy-MM-dd HH:mm:ss`
  * 默认值：无
  * 注意：指定起止时间会自动忽略分钟和秒，转为整点时刻，例如 2019-4-18 的 [3:35, 4:55) 会被转为 [3:00, 4:00)




### 3.3 类型转换

| DataX 内部类型 | TSDB 数据类型                                                |
| -------------- | ------------------------------------------------------------ |
| String         | TSDB 数据点序列化字符串，包括 timestamp、metric、tags、fields 和 value |






## 4 约束限制

### 4.2 如果存在某一个 Metric 下在一个小时范围内的数据量过大，可能需要通过 `-j` 参数调整 JVM 内存大小

考虑到下游 Writer 如果写入速度不及 TSDB Reader 的查询数据，可能会存在积压的情况，因此需要适当地调整 JVM 参数。以"从 阿里云 TSDB 数据库同步抽取数据到本地的作业"为例，启动命令如下：

```bash
 python datax/bin/datax.py tsdb2stream.json -j "-Xms4096m -Xmx4096m"
```



### 4.3 指定起止时间会自动被转为整点时刻

指定起止时间会自动被转为整点时刻，例如 2019-4-18 的 `[3:35, 3:55)` 会被转为 `[3:00, 4:00)`



