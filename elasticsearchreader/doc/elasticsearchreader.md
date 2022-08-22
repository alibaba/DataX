# DataX ElasticSearchReader


---

## 1 快速介绍

[Datax](https://github.com/alibaba/DataX)
读取elasticsearch数据的插件

## 2 实现原理

使用elasticsearch的rest api接口， 批量读取elasticsearch的数据

## 3 功能说明

### 3.1 配置样例

#### es索引示例

```
{
  "flow_id" : 590000001878,
  "taches" : [
    {
      "tch_id" : 590000000750,
      "flow_id" : 590000001878,
      "tch_mod" : 5081
    }
  ],
}
```

#### job.json

```
{
  "core": {
    "container": {
      "job": {
        "reportInterval": 10000
      },
      "taskGroup": {
        "channel": 5
      },
      "trace": {
        "enable": "true"
      }
    }
  },
  "job": {
    "setting": {
      "speed": {
        "byte": 10485760
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0.02
      }
    },
    "content": [
      {
        "reader": {
          "name": "elasticsearchreader",
          "parameter": {
            "endpoint": "http://192.168.17.190:9200",
            "accessId": "xxxx",
            "accessKey": "xxxx",
            "index": "test-datax",
            "type": "default",
            "searchType": "dfs_query_then_fetch",
            "headers": {
            },
            "scroll": "3m",
            "search": [
              {
                "size": 5,
                "query": {
                  "bool": {
                    "must": [
                      {
                        "match": {
                          "_id": "590000001878"
                        }
                      }
                    ]
                  }
                }
              }
            ],
            "table":{
              "name": "TACHE",
              "filter": "pk != null",
              "nameCase": "UPPERCASE",
              "column": [
                {
                  "name": "flow_id",
                  "alias": "pk", 
                },
                {
                  "name": "taches",
                  "child": [
                    {
                      "name": "tch_id"
                    },
                    {
                      "name": "tch_mod"
                    },
                    {
                      "name": "flow_id"
                    }
                  ]
                }
              ]
            }
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print": true,
            "encoding": "UTF-8"
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

* searchType
  * 描述：搜索类型
  * 必选：否
  * 默认值：dfs_query_then_fetch
 
* headers
  * 描述：http请求头
  * 必选：否
  * 默认值：空
  
* scroll
  * 描述：滚动分页配置
  * 必选：否
  * 默认值：空

* search
  * 描述：json格式api搜索数据体
  * 必选：是
  * 默认值：[]

* table
  * 描述: 数据读取规则配置，name命名，nameCase全局字段大小写，filter使用ognl表达式进行过滤
  * 必选: 是
  * 默认值: 无

* column
  * 描述：需要读取的字段，name对应es文档的key，alias为最终记录的字段名如果为空则使用name，value表示字段为常量，child为嵌套对象
  * 必选：是
  * 默认值：无


## 4 性能报告

略

## 5 约束限制

* filter使用ognl表达式，根对象为整个table对象，key为column最终写入的名称

## 6 FAQ