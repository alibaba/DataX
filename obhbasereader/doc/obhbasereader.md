OceanBase的table api为应用提供了ObHBase的访问接口，因此，OceanBase的table api的reader与HBase Reader的结构和配置方法类似。
obhbasereader插件支持sql和hbase api两种读取方式，两种方式存在如下区别：

1. sql方式可以按照分区或者K值进行数据切片，而hbase api方式的数据切片需要用户手动设置。
2. sql方式会将从obhbase读取的kqtv形式的数据转换为单一横行，而hbase api则不做行列转换，直接以kqtv形式将数据传递给下游。
3. sql方式需要配置column属性，hbase api则不需要配置，数据均为固定的kqtv四列。
4. sql方式仅支持获取获得最新或者最旧版本的数据，而hbase api支持获得多版本数据。
#### 脚本配置
```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 3,
        "byte": 104857600
      },
      "errorLimit": {
        "record": 10
      }
    },
    "content": [
      {
        "reader": {
          "name": "obhbasereader",
          "parameter": {
            "username": "username",
            "password": "password",
            "encoding": "utf8",
            "column": [
              {
                "name": "f1:column1_1",
                "type": "string"
              },
              {
                "name": "f1:column2_2",
                "type": "string"
              },
              {
                "name": "f1:column1_1",
                "type": "string"
              },
              {
                "name": "f1:column2_2",
                "type": "string"
              }
            ],
            "range": [
              {
                "startRowkey": "aaa",
                "endRowkey": "ccc",
                "isBinaryRowkey": false
              },
              {
                "startRowkey": "eee",
                "endRowkey": "zzz",
                "isBinaryRowkey": false
              }
            ],
            "mode": "normal",
            "readByPartition": "true",
            "scanCacheSize": "",
            "readerHint": "",
            "readBatchSize": "1000",
            "connection": [
              {
                "table": [
                  "htable1",
                  "htable2"
                ],
                "jdbcUrl": [
                  "||_dsc_ob10_dsc_||集群:租户||_dsc_ob10_dsc_||jdbc:mysql://ip:port/dbName1"
                ],
                "username": "username",
                "password": "password"
              },
              {
                "table": [
                  "htable1",
                  "htable2"
                ],
                "jdbcUrl": [
                  "jdbc:mysql://ip:port/database"
                ]
              }
            ]
          }
        },
        "writer": {
          "name": "txtfilewriter",
          "parameter": {
            "path": "/Users/xujing/datax/txtfile",
            "charset": "UTF-8",
            "fieldDelimiter": ",",
            "fileName": "hbase",
            "nullFormat": "null",
            "writeMode": "truncate"
          }
        }
      }
    ]
  }
}
```
##### 参数解释

- **connection**
   - 描述：配置分库分表的jdbcUrl和分表名。如果一个分库中有多个分表可以用逗号隔开，也可以写成表名[起始序号-截止序号]
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
   - 描述：所选取的需要同步的表。使用JSON的数组描述，因此支持多张表同时抽取。当配置为多张表时，用户自己需保证多张表是同一schema结构，obhbasereader不予检查表是否同一逻辑表。注意，table必须包含在connection配置单元中。
   - 必选：是
   - 默认值：无
- **readByPartition**
   - 描述：使用sql方式读取时，配置**仅**按照分区进行切片。
   - 必须：否
   - 默认值：false
- **partitionName**
   - 描述：使用sql方式读取时，标识仅读取指定分区名的数据，用户需要保证配置的分区名在表结构中真实存在（要求严格大小写）。
   - 必须：否
   - 默认值：无
- **readBatchSize**
   - 描述：使用sql方式读取时，分页大小。
   - 必须：否
   - 默认值：10w
- **fetchSize**
   - 描述：使用sql方式读取时，控制每次读取数据时从结果集中获取的数据行数。
   - 必须：否
   - 默认值：-2147483648
- **scanCacheSize**
   - 描述：使用hbase api读取时，每次rpc从服务器端读取的行数
   - 必须：否
   - 默认值：256
- **readerHint**
   - 描述：obhbasereader使用sql方式读取时使用的hint
   - 必须：否
   - 默认值：/*+READ_CONSISTENCY(weak),QUERY_TIMEOUT(86400000000)*/
- **column**
   - 描述：使用sql方式读取数据时，所配置的表中需要同步的列名集合，使用JSON的数组描述字段信息。
   - 支持列裁剪，即列可以挑选部分列进行导出。
```
支持列换序，即列可以不按照表schema信息进行导出，同时支持通配符*，在使用之前需仔细核对列信息。
```

- 必选：sql方式读取时必选
   - 默认值：无
- **range**
   - 描述**：**指定hbasereader读取的rowkey范围
   - 必须：否
   - 默认值：无
- **username**
   - 描述：访问OceanBase的用户名
   - 必选：是
   - 默认值：无
- **mode**
   - 描述：读取obhbase的模式，normal 模式，即仅读取一个版本的数据。
   - 必选：是
   - 默认值：normal
- **version**
   - 描述：读取obhbase的版本，当前支持oldest、latest模式，分别表示读取最旧和最新的数据。
   - 必须：是
   - 默认值：oldest

一些注意点：
注：如果配置了**partitionName**，则无需再配置readByPartition，即便配置了也会忽略readByPartition选项，而是仅会读取指定分区的数据。
注：如果配置了**readByPartition**，任务将仅按照分区切分任务，而不会再按照K值进行切分。如果是非分区表，则整张表会被当作一个任务而不会再切分。



