# DataX TDengineReader

## 1 快速介绍

TDengineReader 插件实现了 TDengine 读取数据的功能。

## 2 实现原理

TDengineReader 通过TDengine的JDBC driver查询获取数据。

## 3 功能说明

### 3.1 配置样例

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "tdenginereader",
          "parameter": {
            "host": "192.168.1.82",
            "port": 6030,
            "db": "test",
            "user": "root",
            "password": "taosdata",
            "sql": "select * from weather",
            "beginDateTime": "2021-01-01 00:00:00",
            "endDateTime": "2021-01-02 00:00:00",
            "splitInterval": "1h"
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
* **sql**
    * 描述：用来筛选迁移数据的sql <br />
    * 必选：是 <br />
    * 默认值：无 <br />
* **beginDateTime**
    * 描述：TDengine实例的密码 <br />
    * 必选：是 <br />
    * 默认值：无 <br />
* **endDateTime**
    * 描述： <br />
    * 必选：是 <br />
    * 默认值：无 <br />
* **splitInterval**
    * 描述：按照splitInterval来划分task, 每splitInterval创建一个task <br />
    * 必选：否 <br />
    * 默认值：1h <br />

### 3.3 类型转换


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

#### 4.2.4 性能测试小结

1.
2.

## 5 约束限制

## FAQ