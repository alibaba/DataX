# DataX TDengineWriter

## 1 快速介绍

TDengineWriter 插件实现了写入数据到 TDengine 的功能。 在底层实现上， TDengineWriter 通过 JNI的方式调用libtaos.so/tao.dll中的方法，连接 TDengine
数据库实例，并执行schemaless的写入。 TDengineWriter 面向ETL开发工程师，他们使用 TDengineWriter 从数仓导入数据到 TDengine。同时，TDengineWriter
亦可以作为数据迁移工具为DBA等用户提供服务。

## 2 实现原理

TDengineWriter 通过 DataX 框架获取 Reader
生成的协议数据，根据reader的类型解析数据，通过JNI方式调用libtaos.so（或taos.dll）中的方法，使用schemaless的方式写入到TDengine。

## 3 功能说明

### 3.1 配置样例

* 这里使用一份从OpenTSDB产生到 TDengine 导入的数据。

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "opentsdbreader",
          "parameter": {
            "endpoint": "http://192.168.1.180:4242",
            "column": [
              "weather_temperature"
            ],
            "beginDateTime": "2021-01-01 00:00:00",
            "endDateTime": "2021-01-01 01:00:00"
          }
        },
        "writer": {
          "name": "tdenginewriter",
          "parameter": {
            "host": "192.168.1.180",
            "port": 6030,
            "dbname": "test",
            "user": "root",
            "password": "taosdata"
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

* **host**
    * 描述：TDengine实例的host。

        * 必选：是 <br />

        * 默认值：无 <br />
* **port**
    * 描述：TDengine实例的port。
    * 必选：是 <br />
    * 默认值：无 <br />
* **dbname**
    * 描述：目的数据库的名称。

    * 必选：是 <br />

    * 默认值：无 <br />
* **username**
    * 描述：TDengine实例的用户名 <br />
        * 必选：是 <br />
        * 默认值：无 <br />
* **password**
    * 描述：TDengine实例的密码 <br />
        * 必选：是 <br />
        * 默认值：无 <br />

### 3.3 类型转换

目前，由于opentsdbreader将opentsdb的数据统一读取为json字符串，TDengineWriter 在做Opentsdb到TDengine的迁移时，按照以下类型进行处理：

| OpenTSDB数据类型 | DataX 内部类型| TDengine 数据类型 |
| -------- | -----  | -------- |
| timestamp        | Date           | timestamp         |
| Integer（value） | Double         | double            |
| Float（value） | Double | double            |
| String（value） | String   | binary            |
| Integer（tag）   | String         | binary            |
| Float（tag） | String |binary |
| String（tag） | String |binary |

## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征

建表语句：

单行记录类似于：

#### 4.1.2 机器参数

* 执行DataX的机器参数为:
    1. cpu:
    2. mem:
    3. net: 千兆双网卡
    4. disc: DataX 数据不落磁盘，不统计此项

* TDengine数据库机器参数为:
    1. cpu:
    2. mem:
    3. net: 千兆双网卡
    4. disc:

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

1. 这里的单表，主键类型为 bigint(20),自增。
2. batchSize 和 通道个数，对性能影响较大。
3. 16通道，4096批量提交时，出现 full gc 2次。

#### 4.2.4 性能测试小结

1.
2.

## 5 约束限制

## FAQ