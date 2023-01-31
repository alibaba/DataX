# DataX TDengineWriter

简体中文| [English](./tdenginewriter.md)

## 1 快速介绍

TDengineWriter插件实现了写入数据到TDengine数据库目标表的功能。底层实现上，TDengineWriter通过JDBC连接TDengine，按照TDengine的SQL语法，执行insert语句/schemaless语句，将数据写入TDengine。

TDengineWriter可以作为数据迁移工具供DBA将其它数据库的数据导入到TDengine。



## 2 实现原理

TDengineWriter 通过 DataX 框架获取 Reader生成的协议数据，通过JDBC Driver连接TDengine，执行insert语句/schemaless语句，将数据写入TDengine。

在TDengine中，table可以分成超级表、子表、普通表三种类型，超级表和子表包括colum和tag，子表的tag列的值为固定值，普通表与关系型数据库中表的概念一致。（详细请参考：[数据模型](https://www.taosdata.com/docs/cn/v2.0/architecture#model) ）

TDengineWriter支持向超级表、子表、普通表中写入数据，按照table的类型和column参数中是否包含tbname，使用以下方法进行写入：

1. table为超级表，column中指定tbname：使用自动建表的insert语句，使用tbname作为子表的名称。
2. table为超级表，column中未指定tbname：使用schemaless写入，TDengine会根据超级表名、tag值计算一个子表名称。
3. table为子表：使用insert语句写入，ignoreTagUnmatched参数为true时，忽略record中tag值与table的tag值不一致的数据。
4. table为普通表：使用insert语句写入。



## 3 功能说明
### 3.1 配置样例

配置一个写入TDengine的作业
先在TDengine上创建超级表：

```sql
create database if not exists test;
create table test.weather (ts timestamp, temperature int, humidity double) tags(is_normal bool, device_id binary(100), address nchar(100));
```

使用下面的Job配置，将数据写入TDengine：

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column": [
              {
                "type": "string",
                "value": "tb1"
              },
              {
                "type": "date",
                "value": "2022-02-20 12:00:01"
              },
              {
                "type": "long",
                "random": "0, 10"
              },
              {
                "type": "double",
                "random": "0, 10"
              },
              {
                "type": "bool",
                "random": "0, 50"
              },
              {
                "type": "bytes",
                "value": "abcABC123"
              },
              {
                "type": "string",
                "value": "北京朝阳望京"
              }
            ],
            "sliceRecordCount": 1
          }
        },
        "writer": {
          "name": "tdenginewriter",
          "parameter": {
            "username": "root",
            "password": "taosdata",
            "column": [
              "tbname",
              "ts",
              "temperature",
              "humidity",
              "is_normal",
              "device_id",
              "address"
            ],
            "connection": [
              {
                "table": [
                  "weather"
                ],
                "jdbcUrl": "jdbc:TAOS-RS://192.168.56.105:6041/test"
              }
            ],
            "batchSize": 100,
            "ignoreTagsUnmatched": true
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

* jdbcUrl
  * 描述：数据源的JDBC连接信息，TDengine的JDBC信息请参考：[Java连接器的使用](https://www.taosdata.com/docs/cn/v2.0/connector/java#url)
  * 必选：是
  * 默认值：无
* username
  * 描述：用户名
  * 必选：是
  * 默认值：无
* password
  * 描述：用户名的密码
  * 必选：是
  * 默认值：无
* table
  * 描述：表名的集合，table应该包含column参数中的所有列（tbname除外）。注意，column中的tbname会被当作TDengine中子表名使用。
  * 必选：是
  * 默认值：无
* column
  * 描述：字段名的集合，字段的顺序应该与record中column的
  * 必选：是
  * 默认值：无
* batchSize
  * 描述：每batchSize条record为一个batch进行写入
  * 必选：否
  * 默认值：1
* ignoreTagsUnmatched
  * 描述：当table为TDengine中的一张子表，table具有tag值。如果数据的tag值与table的tag值不想等，数据不写入到table中。
  * 必选：否
  * 默认值：false


### 3.3 类型转换

datax中的数据类型，可以映射到TDengine的数据类型

| DataX 内部类型 | TDengine 数据类型                         |
| -------------- | ----------------------------------------- |
| INT            | TINYINT, SMALLINT, INT                    |
| LONG           | TIMESTAMP, TINYINT, SMALLINT, INT, BIGINT |
| DOUBLE         | FLOAT, DOUBLE                             |
| STRING         | TIMESTAMP, BINARY, NCHAR                  |
| BOOL           | BOOL                                      |
| DATE           | TIMESTAMP                                 |
| BYTES          | BINARY                                    |



### 3.4 各数据源到TDengine的参考示例

下面是一些数据源到TDengine进行数据迁移的示例

| 数据迁移示例       | 配置的示例                                                   |
| ------------------ | ------------------------------------------------------------ |
| TDengine到TDengine | [超级表到超级表，指定tbname](../src/test/resources/t2t-1.json) |
| TDengine到TDengine | [超级表到超级表，不指定tbname](../src/test/resources/t2t-2.json) |
| TDengine到TDengine | [超级表到子表](../src/test/resources/t2t-3.json)             |
| TDengine到TDengine | [普通表到普通表](../src/test/resources/t2t-4.json)           |
| RDBMS到TDengine    | [普通表到超级表，指定tbname](../src/test/resources/dm2t-1.json) |
| RDBMS到TDengine    | [普通表到超级表，不指定tbname](../src/test/resources/dm2t-3.json) |
| RDBMS到TDengine    | [普通表到子表](../src/test/resources/dm2t-2.json)            |
| RDBMS到TDengine    | [普通表到普通表](../src/test/resources/dm2t-4.json)          |
| OpenTSDB到TDengine | [metric到普通表](../src/test/resources/o2t-1.json)           |




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

1. 

#### 4.2.4 性能测试小结




## 5 约束限制

1. 



## FAQ

### 源表和目标表的字段顺序一致吗？

是的，TDengineWriter按照column中字段的顺序解析来自datax的数据。
