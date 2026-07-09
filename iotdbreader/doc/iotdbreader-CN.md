# DataX IoTDBReader

## 1 快速介绍

IoTDBReader 插件实现了 IoTDB 读取数据的功能。

## 2 实现原理

IoTDBReader 通过 IoTDB 的 原生java session 查询获取数据。

## 3 功能说明

### 3.1 配置样例

* 配置一个从 IoTDB 抽取数据作业:

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 3
      }
    },
    "content": [
      {
        "reader": {
          "name": "iotdbreader",
          "parameter": {
            "username": "root",
            "password": "root",
            "host": "192.168.150.100",
            "port": 6667,
            "fetchSize": 10000,
            "version": "V_1_0",
            "##": "时间列插入DataX的Record中的位置，默认第0列",
            "timeColumnPosition": 0,
            "##":"写了querySqls 默认下面全部参数失效，适合开发人员, 多个session并行执行多条SQL语句",
            "querySqls":[
            ],
            "device": "root.cgn.device",
            "##":"时间列不属于测点",
            "measurements": ["A5STD","L2RIS014MD","L2VVP003SM5","D1RIS001MD","D1KRT003EU"],
            "where": "time > 2023-03-07 12:00:00 and time < 2024-03-07 19:00:00"
          }
        },
        "writer": {
          "name": "mysqlwriter",
          "parameter": {
            "username": "root",
            "password": "toy123",
            "writeMode": "insert",
            "#需要提前建表": "CREATE TABLE device (`time` DATETIME,`A5STD` DOUBLE,`L2RIS014MD` DOUBLE,`L2VVP003SM5` BOOLEAN,`D1RIS001MD` DOUBLE,`D1KRT003EU` DOUBLE);",
            "column": ["time","A5STD","L2RIS014MD","L2VVP003SM5","D1RIS001MD","D1KRT003EU"],
            "session": [
              "set session sql_mode='ANSI'"
            ],
            "preSql": [
              "delete from device"
            ],
            "connection": [
              {
                "table": [
                  "device"
                ],
                "#": "下面的URL需要把中括号去掉，否则报错，mysqlreader的bug，未修改",
                "jdbcUrl": "jdbc:mysql://localhost:3306/demodb?useUnicode=true&allowPublicKeyRetrieval=true&characterEncoding=utf-8"
              }
            ]
          }
        }
      }
    ]
  }
}
```

* 配置一个自定义 SQL 的数据抽取作业：

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 3
      }
    },
    "content": [
      {
        "reader": {
          "name": "iotdbreader",
          "parameter": {
            "username": "root",
            "password": "root",
            "host": "192.168.150.100",
            "port": 6667,
            "fetchSize": 10000,
            "version": "V_1_0",
            "timeColumnPosition": 0,
            "##":"写了querySqls 默认下面全部参数失效，适合开发人员, 多个session并行执行多条SQL语句",
            "querySqls":[
              "select * from root.cgn.device",
              "select A5STD,L2RIS014MD,L2VVP003SM5,D1RIS001MD,D1KRT003EU from root.cgn.device"
            ],
            "device": "",
            "measurements": "",
            "where": ""
          }
        },
        "writer": {
          "name": "txtfilewriter",
          "parameter": {
            "path": "D:/下载",
            "fileName": "txtText",
            "writeMode": "truncate",
            "dateFormat": "yyyy-MM-dd"
          }
        }
      }
    ]
  }
}
```

### 3.2 参数说明
* username
  * 描述：用户名
  * 必选：是
  * 默认值：无
* password
  * 描述：用户名的密码
  * 必选：是
  * 默认值：无
* host
  * 描述：连接iotdb数据库的主机地址
  * 必选：是
  * 默认值：无
* port
  * 描述：端口
  * 必选：是
  * 默认值：无
* version
  * 描述：iotdb版本
  * 必选：是
  * 默认值：无
* timeColumnPosition
  * 描述：时间列在Record中列的位置
  * 必选：否
  * 默认值：0
* querySqls
  * 描述：直接写多行SQL，可以并行读取，此时下面的参数失效。
  * 必选：否
  * 默认值：
* device
  * 描述：IoTDB中的概念，可理解为mysql中的表。
  * 必选：querySqls为空时必选
  * 默认值：无
* measurements
  * 描述：IoTDB中的概念，可理解为mysql中的字段。
  * 必选：querySqls为空时必选
  * 默认值：无
* where
  * 描述：查询条件
  * 必选：否
  * 默认值：无 

### 3.3 类型转换

| IoTDB 数据类型      | DataX 内部类型 |
|-----------------|--------|
| INT32           | Int    |
| INT64,TIMESTAMP | Long   |
| FLOAT           | FLOAT  |
| DOUBLE          | Double |
| BOOLEAN         | Bool   |
| DATE            | Date   |
| STRING,TEXT     | String |

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