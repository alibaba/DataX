
# TSDBWriter 插件文档

___


## 1 快速介绍

TSDBWriter 插件实现了将数据点写入到阿里巴巴云原生多模数据库Lindorm TSDB数据库中（后续简称 TSDB）。


时间序列数据库（Time Series Database , 简称 TSDB）是一种高性能，低成本，稳定可靠的在线时序数据库服务；提供高效读写，高压缩比存储、时序数据插值及聚合计算，广泛应用于物联网（IoT）设备监控系统 ，企业能源管理系统（EMS），生产安全监控系统，电力检测系统等行业场景。 TSDB 提供千万级时序数据秒级写入，高压缩比低成本存储、预降采样、插值、多维聚合计算，查询结果可视化功能；解决由于设备采集点数量巨大，数据采集频率高，造成的存储成本高，写入和查询分析效率低的问题。更多关于 TSDB 的介绍，详见[阿里云 Lindorm TSDB 官网](https://help.aliyun.com/document_detail/174600.html)。

注意：阿里巴巴自研HiTSDB已全新升级为云原生多模数据库Lindorm TSDB。Lindorm TSDB兼容大部分HiTSDB的HTTP API并提供原生SQL能力，TSDBWriter插件使用HTTP API方式写入，要使用原生SQL能力需要提前在Lindorm TSDB进行建表。详细参见[与旧版TSDB的比较](https://help.aliyun.com/document_detail/387477.html)


## 2 实现原理

通过TSDB客户端 hitsdb-client 连接 TSDB 实例，并将数据点通过HTTP API方式写入。关于写入接口详见TSDB 的[SDK 参考](https://help.aliyun.com/document_detail/61587.html)。

## 3 功能说明

### 3.1 配置样例

* 配置TSDB Writer：
```json
{
  "name": "tsdbwriter",
  "parameter": {
    "endpoint": "http://localhost:8242",
    "sourceDbType": "RDB",
    "batchSize": 256,
    "columnType": [
      "tag",
      "tag",
      "field_string",
      "field_double",
      "timestamp",
      "field_bool"
    ],
    "column": [
      "tag1",
      "tag2",
      "field1",
      "field2",
      "timestamp",
      "field3"
    ],
    "multiField":"true",
    "table":"testmetric", 
    "username":"xxx",
    "password":"xxx",
    "ignoreWriteError":"false",
    "database":"default"
  }
}
```

* 配置一个从 支持 OpenTSDB 协议的数据库同步抽取数据到 TSDB：

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "opentsdbreader",
          "parameter": {
            "endpoint": "http://localhost:4242",
            "column": [
              "m"
            ],
            "startTime": "2019-01-01 00:00:00",
            "endTime": "2019-01-01 03:00:00"
          }
        },
        "writer": {
          "name": "tsdbwriter",
          "parameter": {
            "endpoint": "http://localhost:8242"
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

* 使用 OpenTSDB （单值）协议写入TSDB（不推荐）：
```json
{
  "name": "tsdbwriter",
  "endpoint": "http://localhost:8242",
  "sourceDbType": "RDB",
  "parameter": {
    "batchSize": 256,
    "columnType": [
      "tag",
      "tag",
      "field_string",
      "field_double",
      "timestamp",
      "field_boolean"
    ],
    "column": [
      "tag1",
      "tag2",
      "field_metric_1",
      "field_metric_2",
      "timestamp",
      "field_metric_3"
    ],
    "username":"tsdb",
    "password":"enxU^",
    "ignoreWriteError":"false"
  }
}
```
转换到的TSDB 表名（metric）由column中field对应的列名决定：对于上诉配置一行关系型数据将会写入三个metric（field_metric_1，field_metric_2，field_metric_3）

### 3.2 参数说明

* **name**
  * 描述：本插件的名称
  * 必选：是
  * 默认值：tsdbhttpwriter

* **parameter**
  * **endpoint**
    * 描述：TSDB 的 HTTP 连接地址
    * 必选：是
    * 格式：http://IP:Port
    * 默认值：无

* **sourceDbType**
   * 描述：源端数据类型
   * 必选：否
   * 格式：string [RDB或者TSDB]
   * 默认值：TSDB

* **multiField**
   * 描述：使用HTTP API多值（多个field）方式写入，目前TSDB版本使用多值写入，需要指定为true
   * 必选：是
   * 格式：bool
   * 默认值：false （单值）
   * 说明: 如果使用Lindorm TSDB原生SQL能力访问HTTP API方式写入的数据，需要在TSDB进行预建表，否则只能使用HiTSDB HTTP API方式[查询数据](https://help.aliyun.com/document_detail/107576.html)。

* **column**
   * 描述：关系型数据库中表的字段名
   * 必选：当sourceDbType为RDB时必选
   * 格式：string 
   * 默认值：无
   * 说明: 此处的字段顺序，需要和Reader插件中配置的column字段的顺序保持一致。

* **columnType**
   * 描述：关系型数据库中表字段，映射到TSDB中的类型。支持的类型如下所示：
      * timestamp：该字段为时间戳
      * tag：该字段为tag
      * field_string: 该Field的value是字符串类型
      * field_double: 该Field的value是数值类型
      * field_boolean: 该Field的value是布尔类型
   * 必选：当sourceDbType为RDB时必选
   * 格式：string [RDB或者TSDB]
   * 默认值: 无
   * 说明: 此处的字段顺序，需要和column配置中的字段顺序保持一致

* **table**
   * 描述：TSDB对应表名（metric）
   * 必选：当sourceDbType为RDB时且multiField为true时必选
   * 格式：string 
   * 默认值：无
   * 说明: 要导入的TSDB表名，如果multiField为falase,不需要填写，对应的metric需要写到column字段

* **batchSize**
   * 描述：每次批量数据的条数
   * 必选：否
   * 格式：int，需要保证大于 0
   * 默认值：100

* **ignoreWriteError**
   * 描述：如果设置为 true，则忽略写入错误，继续写入；否则，多次重试后仍写入失败的话，则会终止写入任务
   * 必选：否
   * 格式：bool
   * 默认值：false

* **username**
   * 描述：数据库用户名
   * 必选：否
   * 格式：string 
   * 默认值：无
   * 说明: TSDB配置了鉴权需要填写


* **password**
   * 描述：数据库密码
   * 必选：否
   * 格式：string 
   * 默认值：无
   * 说明: TSDB配置了鉴权需要填写

* **database**
   * 描述：导入的数据库
   * 必选：否
   * 格式：string 
   * 默认值：default
   * 说明: TSDB需要提前创建数据库

* **maxRetryTime**
   * 描述：失败后重试的次数
   * 必选：否
   * 格式：int，需要保证大于 1
   * 默认值：3







### 3.3 类型转换


| DataX 内部类型 | TSDB 数据类型                                                |
| -------------- | ------------------------------------------------------------ |
| String         | TSDB 数据点序列化字符串，包括 timestamp、metric、tags 和 value |






## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征

从 Metric、时间线、Value 和 采集周期 四个方面来描述：

##### metric

固定指定一个 metric 为 `m`。

##### tagkv

前四个 tagkv 全排列，形成 `10 * 20 * 100 * 100 = 2000000` 条时间线，最后 IP 对应 2000000 条时间线从 1 开始自增。

| **tag_k** | **tag_v**     |
| --------- | ------------- |
| zone      | z1~z10        |
| cluster   | c1~c20        |
| group     | g1~100        |
| app       | a1~a100       |
| ip        | ip1~ip2000000 |

##### value

度量值为 [1, 100] 区间内的随机值

##### interval

采集周期为 10 秒，持续摄入 3 小时，总数据量为 `3 * 60 * 60 / 10 * 2000000 = 2,160,000,000` 个数据点。



#### 4.1.2 机器参数

TSDB Writer 机型:  64C256G

HBase 机型： 8C16G * 5

#### 4.1.3 DataX jvm 参数

"-Xms4096m -Xmx4096m"




### 4.2 测试报告


| 通道数 | DataX 速度 (Rec/s) | DataX 流量 (MB/s) |
| ------ | ------------------ | ----------------- |
| 1      | 129753             | 15.45             |
| 2      | 284953             | 33.70             |
| 3      | 385868             | 45.71             |





## 5 约束限制

### 5.1 目前支持Lindorm TSDB全部版本 以及 HiTSDB 2.4.x 及以上版本

其他版本暂不保证兼容





## 6 FAQ





