# DataX ElasticSearchWriter


---

## 1 快速介绍

数据导入elasticsearch的插件

## 2 实现原理

使用elasticsearch的rest api接口， 批量把从reader读入的数据写入elasticsearch

## 3 功能说明

### 3.1 配置样例

#### job.json

```
{
  "job": {
    "setting": {
        "speed": {
            "channel": 1
        }
    },
    "content": [
      {
        "reader": {
          ...
        },
        "writer": {
          "name": "elasticsearchwriter",
          "parameter": {
            "endpoint": "http://xxx:9999",
            "accessId": "xxxx",
            "accessKey": "xxxx",
            "index": "test-1",
            "type": "default",
            "cleanup": true,
            "settings": {"index" :{"number_of_shards": 1, "number_of_replicas": 0}},
            "discovery": false,
            "batchSize": 1000,
            "splitter": ",",
            "column": [
              {"name": "pk", "type": "id"},
              { "name": "col_ip","type": "ip" },
              { "name": "col_double","type": "double" },
              { "name": "col_long","type": "long" },
              { "name": "col_integer","type": "integer" },
              { "name": "col_keyword", "type": "keyword" },
              { "name": "col_text", "type": "text", "analyzer": "ik_max_word"},
              { "name": "col_geo_point", "type": "geo_point" },
              { "name": "col_date", "type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
              { "name": "col_nested1", "type": "nested" },
              { "name": "col_nested2", "type": "nested" },
              { "name": "col_object1", "type": "object" },
              { "name": "col_object2", "type": "object" },
              { "name": "col_integer_array", "type":"integer", "array":true},
              { "name": "col_geo_shape", "type":"geo_shape", "tree": "quadtree", "precision": "10m"}
            ]
          }
        }
      }
    ]
  }
}
```

#### 3.2 参数说明

* endpoint
 * 描述：ElasticSearch的连接地址
 * 必选：是
 * 默认值：无

* accessId
 * 描述：http auth中的user
 * 必选：否
 * 默认值：空

* accessKey
 * 描述：http auth中的password
 * 必选：否
 * 默认值：空

* index
 * 描述：elasticsearch中的index名
 * 必选：是
 * 默认值：无

* type
 * 描述：elasticsearch中index的type名
 * 必选：否
 * 默认值：index名

* cleanup
 * 描述：是否删除原表
 * 必选：否
 * 默认值：false

* batchSize
 * 描述：每次批量数据的条数
 * 必选：否
 * 默认值：1000

* trySize
 * 描述：失败后重试的次数
 * 必选：否
 * 默认值：30

* timeout
 * 描述：客户端超时时间
 * 必选：否
 * 默认值：600000

* discovery
 * 描述：启用节点发现将(轮询)并定期更新客户机中的服务器列表。
 * 必选：否
 * 默认值：false

* compression
 * 描述：http请求，开启压缩
 * 必选：否
 * 默认值：true

* multiThread
 * 描述：http请求，是否有多线程
 * 必选：否
 * 默认值：true

* ignoreWriteError
 * 描述：忽略写入错误，不重试，继续写入
 * 必选：否
 * 默认值：false

* ignoreParseError
 * 描述：忽略解析数据格式错误，继续写入
 * 必选：否
 * 默认值：true

* alias
 * 描述：数据导入完成后写入别名
 * 必选：否
 * 默认值：无

* aliasMode
 * 描述：数据导入完成后增加别名的模式，append(增加模式), exclusive(只留这一个)
 * 必选：否
 * 默认值：append

* settings
 * 描述：创建index时候的settings, 与elasticsearch官方相同
 * 必选：否
 * 默认值：无

* splitter
 * 描述：如果插入数据是array，就使用指定分隔符
 * 必选：否
 * 默认值：-,-

* column
 * 描述：elasticsearch所支持的字段类型，样例中包含了全部
 * 必选：是

* dynamic
 * 描述: 不使用datax的mappings，使用es自己的自动mappings
 * 必选: 否
 * 默认值: false



## 4 性能报告

### 4.1 环境准备

* 总数据量 1kw条数据, 每条0.1kb
* 1个shard, 0个replica
* 不加id，这样默认是append_only模式，不检查版本，插入速度会有20%左右的提升

#### 4.1.1 输入数据类型(streamreader)

```
{"value": "1.1.1.1", "type": "string"},
{"value": 19890604.0, "type": "double"},
{"value": 19890604, "type": "long"},
{"value": 19890604, "type": "long"},
{"value": "hello world", "type": "string"},
{"value": "hello world", "type": "string"},
{"value": "41.12,-71.34", "type": "string"},
{"value": "2017-05-25", "type": "string"},
```

#### 4.1.2 输出数据类型(eswriter)

```
{ "name": "col_ip","type": "ip" },
{ "name": "col_double","type": "double" },
{ "name": "col_long","type": "long" },
{ "name": "col_integer","type": "integer" },
{ "name": "col_keyword", "type": "keyword" },
{ "name": "col_text", "type": "text"},
{ "name": "col_geo_point", "type": "geo_point" },
{ "name": "col_date", "type": "date"}
```

#### 4.1.2 机器参数

1. cpu: 32  Intel(R) Xeon(R) CPU E5-2650 v2 @ 2.60GHz
2. mem: 128G
3. net: 千兆双网卡

#### 4.1.3 DataX jvm 参数

-Xms1024m -Xmx1024m -XX:+HeapDumpOnOutOfMemoryError

### 4.2 测试报告

| 通道数|  批量提交行数| DataX速度(Rec/s)|DataX流量(MB/s)|
|--------|--------| --------|--------|
| 4| 256| 11013| 0.828|
| 4| 1024| 19417| 1.43|
| 4| 4096| 23923| 1.76|
| 4| 8172| 24449| 1.80|
| 8| 256| 21459| 1.58|
| 8| 1024| 37037| 2.72|
| 8| 4096| 45454| 3.34|
| 8| 8172| 45871| 3.37|
| 16| 1024| 67567| 4.96|
| 16| 4096| 78125| 5.74|
| 16| 8172| 77519| 5.69|
| 32| 1024| 94339| 6.93|
| 32| 4096| 96153| 7.06|
| 64| 1024| 91743| 6.74|

### 4.3 测试总结

* 最好的结果是32通道，每次传4096，如果单条数据很大， 请适当减少批量数，防止oom
* 当然这个很容易水平扩展，而且es也是分布式的，多设置几个shard也可以水平扩展

## 5 约束限制

* 如果导入id，这样数据导入失败也会重试，重新导入也仅仅是覆盖，保证数据一致性
* 如果不导入id，就是append_only模式，elasticsearch自动生成id，速度会提升20%左右，但数据无法修复，适合日志型数据(对数据精度要求不高的)