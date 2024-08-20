OceanBase的table api为应用提供了ObHBase的访问接口，因此，OceanBase table api的reader与HBase writer的结构和配置方法类似。
1 快速介绍
obhbaseWriter 插件实现了从向ObHbase中写取数据。在底层实现上，obhbaseWriter 通过 HBase 的 Java 客户端连接远程 HBase 服务，并通过 put 方式写入obHbase。
1.1支持功能
1、目前obhbasewriter支持的obHbase版本为OceanBase3.x以及4.x版本。
2、目前obhbasewriter支持源端多个字段拼接作为ObHbase 表的 rowkey，具体配置参考：rowkeyColumn配置；
3、写入obhbase的时间戳（版本）支持：用当前时间作为版本，指定源端列作为版本，指定一个时间 三种方式作为版本；
#### 脚本配置
```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 5
      }
    },
    "content": [
      {
        "reader": {
          "name": "txtfilereader",
          "parameter": {
            "path": "/normal.txt",
            "charset": "UTF-8",
            "column": [
              {
                "index": 0,
                "type": "String"
              },
              {
                "index": 1,
                "type": "string"
              },
              {
                "index": 2,
                "type": "string"
              },
              {
                "index": 3,
                "type": "string"
              },
              {
                "index": 4,
                "type": "string"
              },
              {
                "index": 5,
                "type": "string"
              },
              {
                "index": 6,
                "type": "string"
              }

            ],
            "fieldDelimiter": ","
          }
        },
        "writer": {
          "name": "obhbasewriter",
          "parameter": {
            "username": "username",
            "password": "password",
            "writerThreadCount": "20",
            "writeBufferHighMark": "2147483647",
            "rpcExecuteTimeout": "30000",
            "useOdpMode": "false",
            "obSysUser": "root",
            "obSysPassword": "",
            "column": [
              {
                "index": 0,
                "name": "family1:c1",
                "type": "string"
              },
              {
                "index": 1,
                "name": "family1:c2",
                "type": "string"
              },
              {
                "index": 2,
                "name": "family1:c3",
                "type": "string"
              },
              {
                "index": 3,
                "name": "family1:c4",
                "type": "string"
              },
              {
                "index": 4,
                "name": "family1:c5",
                "type": "string"
              },
              {
                "index": 5,
                "name": "family1:c6",
                "type": "string"
              },
              {
                "index": 6,
                "name": "family1:c7",
                "type": "string"
              }
            ],
            "mode": "normal",
            "rowkeyColumn": [
              {
                "index": 0,
                "type": "string"
              },
              {
                "index": 3,
                "type": "string"
              },
              {
                "index": 2,
                "type": "string"
              },
              {
                "index": 1,
                "type": "string"
              }
            ],
            "table": "htable3",
            "batchSize": "200",
            "dbName": "database",
            "jdbcUrl": "jdbc:mysql://ip:port/database?"
          }
        }
      }
    ]
  }
}
```
##### 参数解释

- **connection**

公有云和私有云需要配置的信息不同，具体如下：
公有云：

- 数据库用户名；（在外层统一配置）
- 用户密码；（在外层统一配置）
- proxy的jdbc地址
- 数据库名称；

私有云：

- 数据库用户名；（在外层统一配置）
- 用户密码；（在外层统一配置）
- proxy的jdbc地址
- obSysUser：sys租户的用户名；
- obSysPass：sys租户的密码；
- configUrl；
    - 描述：可以通过show parameters like 'obConfigUrl' 获得。
    - 必须：是
    - 默认值：无
- **jdbcUrl**
    - 描述：连接ob使用的jdbc url，支持如下两种格式：
      - jdbc:mysql://obproxyIp:obproxyPort/db
        - 此格式下username需要写成三段式格式
      - ||_dsc_ob10_dsc_||集群名:租户名||_dsc_ob10_dsc_||jdbc:mysql://obproxyIp:obproxyPort/db
        - 此格式下username仅填写用户名本身，无需三段式写法
   - 必选：是
   - 默认值：无
- **table**
    - 描述：所选取的需要同步的表。无需增加列族信息。
    - 必选：是
   - 默认值：无
- **username**
    - 描述：访问OceanBase的用户名
    - 必选：是
   - 默认值：无
- **useOdpMode**
    - 描述：是否通过proxy连接。无法提供sys租户帐密时需要设置为true
    - 必须：否
    - 默认值：false
- **column**
    - 描述：要写入的hbase字段。index：指定该列对应reader端column的索引，从0开始；name：指定hbase表中的列，必须为 列族:列名 的格式；type：指定写入数据类型，用于转换HBase byte[]。配置格式如下：
```json
"column": [ { "index":1, "name": "cf1:q1", "type": "string" }, { "index":2, "name": "cf1:q2", "type": "string" } ］
```

- 必选：是
   - 默认值：无
- **rowkeyColumn**
    - 描述：要写入的ObHbase的rowkey列。index：指定该列对应reader端column的索引，从0开始，若为常量index为－1；type：指定写入数据类型，用于转换HBase byte[]；value：配置常量，常作为多个字段的拼接符。obhbasewriter会将rowkeyColumn中所有列按照配置顺序进行拼接作为写入hbase的rowkey，不能全为常量。配置格式如下：
```json
"rowkeyColumn": [ { "index":0, "type":"string" }, { "index":-1, "type":"string", "value":"_" } ]
```

- 必选：是
   - 默认值：无
- **versionColumn**
    - 描述：指定写入obhbase的时间戳。支持：当前时间、指定时间列，指定时间，三者选一。若不配置表示用当前时间。index：指定对应reader端column的索引，从0开始，需保证能转换为long,若是Date类型，会尝试用yyyy-MM-dd HH:mm:ss和yyyy-MM-dd HH:mm:ss SSS去解析；若为指定时间index为－1；value：指定时间的值,long值。配置格式如下：
```json
"versionColumn":{ "index":1 }
```
或者
```json
"versionColumn":{ "index":－1, "value":123456789 }
```

- 必选：否
- 默认值：无



