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