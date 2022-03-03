# DataX TDengineWriter

简体中文| [English](./tdenginewriter.md)

## 1 快速介绍

TDengineWriter插件实现了写入数据到TDengine数据库功能。可用于离线同步其它数据库的数据到TDengine。

## 2 实现原理

TDengineWriter 通过 DataX 框架获取 Reader生成的协议数据，根据reader的类型解析数据。目前有两种写入方式：

1. 对于OpenTSDBReader, TDengineWriter通过JNI方式调用TDengine客户端库文件（taos.lib或taos.dll）中的方法，使用[schemaless的方式](https://www.taosdata.com/cn/documentation/insert#schemaless)写入。

2. 对于其它数据源,会根据配置生成SQL语句, 通过[taos-jdbcdriver](https://www.taosdata.com/cn/documentation/connector/java)批量写入。

这样区分的原因是OpenTSDBReader将opentsdb的数据统一读取为json字符串，Writer端接收到的数据只有1列。而其它Reader插件一般会把数据放在不同列。

## 3 功能说明
### 3.1 从OpenTSDB到TDengine
#### 3.1.1 配置样例

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
            "dbName": "test",
            "username": "root",
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

#### 3.1.2 参数说明

| 参数      | 描述                 | 是否必选 | 默认值   |
| --------- | -------------------- | -------- | -------- |
| host      | TDengine实例的host   | 是       | 无       |
| port      | TDengine实例的port   | 是       | 无       |
| username      | TDengine实例的用户名 | 否       | root     |
| password  | TDengine实例的密码   | 否       | taosdata |
| dbName    | 目的数据库的名称     | 是       | 无       |
| batchSize | 每次批量插入多少记录 | 否       | 1        |


#### 3.1.3 类型转换

目前，由于OpenTSDBReader将opentsdb的数据统一读取为json字符串，TDengineWriter 在做Opentsdb到TDengine的迁移时，按照以下类型进行处理：

| OpenTSDB数据类型 | DataX 内部类型 | TDengine 数据类型 |
| ---------------- | -------------- | ----------------- |
| timestamp        | Date           | timestamp         |
| Integer（value） | Double         | double            |
| Float（value）   | Double         | double            |
| String（value）  | String         | binary            |
| Integer（tag）   | String         | binary            |
| Float（tag）     | String         | binary            |
| String（tag）    | String         | binary            |

### 3.2 从MongoDB到TDengine

#### 3.2.1 配置样例
```json
{
    "job": {
        "setting": {
            "speed": {
                "channel": 2
            }
        },
        "content": [
            {
                "reader": {
                    "name": "mongodbreader",
                    "parameter": {
                        "address": [
                            "127.0.0.1:27017"
                        ],
                        "userName": "user",
                        "mechanism": "SCRAM-SHA-1",
                        "userPassword": "password",
                        "authDb": "admin",
                        "dbName": "test",
                        "collectionName": "stock",
                        "column": [
                            {
                              "name": "stockID",
                              "type": "string"  
                            },
                            {
                                "name": "tradeTime",
                                "type": "date"
                            },
                            {
                                "name": "lastPrice",
                                "type": "double"
                            },
                            {
                              "name": "askPrice1",
                              "type": "double"
                            },
                            {
                              "name": "bidPrice1",
                              "type": "double"
                            },
                            {
                                "name": "volume",
                                "type": "int"
                            }
                        ]
                    }
                },
                "writer": {
                    "name": "tdenginewriter",
                    "parameter": {
                        "host": "localhost",
                        "port": 6030,
                        "dbName": "test",
                        "username": "root",
                        "password": "taosdata",
                        "stable": "stock",
                        "tagColumn": {
                          "industry": "energy",
                          "stockID": 0
                        },
                        "fieldColumn": {
                          "lastPrice": 2,
                          "askPrice1": 3,
                          "bidPrice1": 4,
                          "volume": 5
                        },
                        "timestampColumn": {
                          "tradeTime": 1
                        }
                    }
                }
            }
        ]
    }
}
```

**注：本配置的writer部分同样适用于关系型数据库**


#### 3.2.2 参数说明
| 参数            | 描述                 | 是否必选         | 默认值   | 备注               |
| --------------- | -------------------- | ---------------- | -------- | ------------------ |
| host            | TDengine实例的host   | 是               | 无       |
| port            | TDengine实例的port   | 是               | 无       |
| username            | TDengine实例的用户名 | 否               | root     |
| password        | TDengine实例的密码   | 否               | taosdata |
| dbName          | 目的数据库的名称     | 是               | 无       |
| batchSize       | 每次批量插入多少记录 | 否               | 1000     |
| stable          | 目标超级表的名称     | 是(OpenTSDB除外) | 无       |
| tagColumn       | 格式:{tagName1: tagInd1, tagName2: tagInd2}, 标签列在写插件收到的Record中的位置和列名   | 否               | 无       | 位置索引均从0开始, tagInd如果为字符串, 表示固定标签值，不需要从源数据中获取  |
| fieldColumn     | 格式:{fdName1: fdInd1, fdName2: fdInd2}, 字段列在写插件收到的Record中的位置和列名   | 否               | 无       |                    |
| timestampColumn | 格式:{tsColName: tsColIndex}, 时间戳列在写插件收到的Record中的位置和列名 | 否               | 无       | 时间戳列只能有一个 |

示例配置中tagColumn有一个industry，它的值是一个固定的字符串“energy”, 作用是给导入的所有数据加一个值为"energy"的固定标签industry。这个应用场景可以是：在源库中，有多个设备采集的数据分表存储，设备名就是表名，可以用这个机制把设备名称转化为标签。

#### 3.2.3 自动建表规则
##### 3.2.3.1 超级表创建规则

如果配置了tagColumn、 fieldColumn和timestampColumn将会在插入第一条数据前，自动创建超级表。<br>
数据列的类型从第1条记录自动推断, 标签列默认类型为`NCHAR(64)`, 比如示例配置，可能生成以下建表语句：

```sql
CREATE STABLE IF NOT EXISTS market_snapshot (
  tadetime TIMESTAMP,
  lastprice DOUBLE,
  askprice1 DOUBLE,
  bidprice1 DOUBLE,
  volume INT
)
TAGS(
  industry NCHAR(64),
  stockID NCHAR(64)
);
```

##### 3.2.3.2 子表创建规则

子表结构与超级表相同，子表表名生成规则：
1. 将标签的value 组合成为如下的字符串: `tag_value1!tag_value2!tag_value3`。
2. 计算该字符串的 MD5 散列值 "md5_val"。
3. "t_md5val"作为子表名。其中的 "t" 是固定的前缀。

#### 3.2.4 用户提前建表

如果你已经创建好目标超级表，那么tagColumn、 fieldColumn和timestampColumn三个字段均可省略, 插件将通过执行通过`describe stableName`获取表结构的信息。
此时要求接收到的Record中Column的顺序和执行`describe stableName`返回的列顺序相同， 比如通过`describe stableName`返回以下内容：
```
             Field              |         Type         |   Length    |   Note   |
=================================================================================
 ts                             | TIMESTAMP            |           8 |          |
 current                        | DOUBLE               |           8 |          |
 location                      | BINARY                |           10 | TAG      |
```
那么插件收到的数据第1列必须代表时间戳，第2列必须代表电流，第3列必须代表位置。

#### 3.2.5 注意事项

1. tagColumn、 fieldColumn和timestampColumn三个字段用于描述目标表的结构信息，这三个配置字段必须同时存在或同时省略。
2. 如果存在以上三个配置，且目标表也已经存在，则两者必须一致。**一致性**由用户自己保证，插件不做检查。不一致可能会导致插入失败或插入数据错乱。

#### 3.2.6 类型转换

| DataX 内部类型 | TDengine 数据类型 |
|-------------- | ----------------- |
|Long           | BIGINT            |
|Double         | DOUBLE            |
|String         | NCHAR(64)         |
|Date           | TIMESTAMP         |
|Boolean        | BOOL              |
|Bytes          | BINARY(64)        |

### 3.3 从关系型数据库到TDengine
writer部分的配置规则和上述MongoDB的示例是一样的，这里给出一个MySQL的示例。

#### 3.3.1 MySQL中表结构
```sql
CREATE TABLE IF NOT EXISTS weather(
    station varchar(100),
    latitude DOUBLE,
    longtitude DOUBLE,
    `date` DATE,
    TMAX int,
    TMIN int
)
```

#### 3.3.2 配置文件示例

```json 
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "username": "root",
            "password": "passw0rd",
            "column": [
              "*"
            ],
            "splitPk": "station",
            "connection": [
              {
                "table": [
                  "weather"
                ],
                "jdbcUrl": [
                  "jdbc:mysql://127.0.0.1:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8"
                ]
              }
            ]
          }
        },
        "writer": {
          "name": "tdenginewriter",
          "parameter": {
            "host": "127.0.0.1",
            "port": 6030,
            "dbName": "test",
            "username": "root",
            "password": "taosdata",
            "batchSize": 1000,
            "stable": "weather",
            "tagColumn": {
              "station": 0
            },
            "fieldColumn": {
              "latitude": 1,
              "longtitude": 2,
              "tmax": 4,
              "tmin": 5
            },
            "timestampColumn":{
              "date": 3
            }
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

| 通道数 | DataX速度(Rec/s) | DataX流量(MB/s) | DataX机器网卡流出流量(MB/s) | DataX机器运行负载 | DB网卡进入流量(MB/s) | DB运行负载 | DB TPS |
| ------ | ---------------- | --------------- | --------------------------- | ----------------- | -------------------- | ---------- | ------ |
| 1      |                  |                 |                             |                   |                      |            |        |
| 4      |                  |                 |                             |                   |                      |            |        |
| 8      |                  |                 |                             |                   |                      |            |        |
| 16     |                  |                 |                             |                   |                      |            |        |
| 32     |                  |                 |                             |                   |                      |            |        |

说明：

1. 这里的单表，主键类型为 bigint(20),自增。
2. batchSize 和 通道个数，对性能影响较大。
3. 16通道，4096批量提交时，出现 full gc 2次。

#### 4.2.4 性能测试小结


## 5 约束限制

1. 本插件自动创建超级表时NCHAR类型的长度固定为64，对于包含长度大于64的字符串的数据源，将不支持。
2. 标签列不能包含null值，如果包含会被过滤掉。

## FAQ

### 如何选取要同步的数据的范围？

数据范围的选取在Reader插件端配置，对于不同的Reader插件配置方法往往不同。比如对于mysqlreader， 可以用sql语句指定数据范围。对于opentsdbreader, 用beginDateTime和endDateTime两个配置项指定数据范围。

### 如何一次导入多张源表？

如果Reader插件支持一次读多张表，Writer插件就能一次导入多张表。如果Reader不支持多多张表，可以建多个job，分别导入。Writer插件只负责写数据。

### 一张源表导入之后对应TDengine中多少张表？

这是由tagColumn决定的，如果所有tag列的值都相同，那么目标表只有一个。源表有多少不同的tag组合，目标超级表就有多少子表。

### 源表和目标表的字段顺序一致吗？

TDengine要求每个表第一列是时间戳列，后边是普通字段，最后是标签列。如果源表不是这个顺序，插件在自动建表时会自动调整。

### 插件如何确定各列的数据类型？

根据收到的第一批数据自动推断各列的类型。

### 为什么插入10年前的数据会抛异常`TDengine ERROR (2350): failed to execute batch bind` ?

因为创建数据库的时候，默认保留10年的数据。可以手动指定要保留多长时间的数据，比如:`CREATE DATABASE power KEEP 36500;`。

### 如果编译的时候某些插件的依赖找不到怎么办？

如果这个插件不是必须的，可以注释掉根目录下的pom.xml中的对应插件。