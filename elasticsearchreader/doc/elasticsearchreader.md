# DataX ElasticSearchReader


---

## 1 快速介绍

数据读取elasticsearch的插件

## 2 实现原理

ESreader插件实现了从elasticsearch读取数据。在底层实现上，ESreader通过http连接，并执行相应的DSL语句将数据从elasticsearch中查询出来。
根据shardsNumber切分channel，最大并发为shardsNumber数量

## 3 功能说明

### 3.1 配置样例

#### job.json

```
{
    "job":{
        "setting":{
            "speed":{
                "byte":"1024",
                "channel":"2"
            }
        },
        "content":[
            {
                "reader":{
                    "name":"elasticsearchreader",
                    "parameter":{
                        "endpoint":"http://10.22.22.22:9200",
                        "index":"app_index_v1",
                        "type":"doc",
                        "shardNum":3,
                        "accessId":"defaultid",
                        "accessKey":"defaultid",
                        "pageSize":10,
                        "column":[
                            "sales",
                            "brandName"
                        ],
                        "where":{
                            "term":{
                                "brandName":"宝路"
                            }
                        }
                    }
                },
                "writer":{
                    "name":"mysqlwriter",
                    "parameter":{
                        "writeMode":"insert",
                        "username":"******",
                        "password":"******",
                        "column":[
                            "id",
                            "name"
                        ],
                        "preSql":[
                            "delete from es_reader_test_jest"
                        ],
                        "session":[
                            "set session sql_mode='ANSI'"
                        ],
                        "connection":[
                            {
                                "table":[
                                    "es_reader_test_jest"
                                ],
                                "jdbcUrl":"jdbc:mysql://10.251.136.13:3306/test?useSSL=false&autoReconnect=true&useUnicode=true&characterEncoding=UTF-8"
                            }
                        ]
                    }
                }
            }
        ]
    }
}
```

#### 3.2 参数说明

o	endpoint
-	描述：ElasticSearch的连接地址
-	必选：是
-	默认值：无
o	accessId
-	描述：http auth中的user
-	必选：否
-	默认值：defaultaccessId
o	accessKey
-	描述：http auth中的password
-	必选：否
-	默认值：defaultaccessKey
o	index
-	描述：elasticsearch中的index名
-	必选：是
-	默认值：无
o	type
-	描述：elasticsearch中index的type名
-	必选：否
-	默认值：无
o	shardNum
-	描述：elasticsearch中分片数量
-	必选：是
-	默认值：无
o	pageSize
-	描述：scroll-scan查询中每个分片上结果数目，每个批次实际返回的文档数量最大为 size * shardNum 
-	必选：否
-	默认值：1000
o	where
-	描述：数据过滤条件
-	格式：Elasticsearch DSL 常用语法
"term":{    "brandName":"宝路"     }
o	必选：否
o	默认值：无




## 4 性能报告




### 4.2 测试报告

数据量	|pageSize	|时间	|记录写入速度
1000/85.2ki	|100	|11s	|333rec/s
59877/13.0Mi	|1000	|10s	|9979rec/s
59877/13.0Mi	|10000	|11s	|9979rec/s
376982/49.5Mi	|1000	|32s	|13962rec/s
376982/49.5Mi	|10000	|25s	|17951rec/s
3805010/516Mi 	|100	|969s	|3947rec/s
3805010/516Mi 	|1000	|325s	|11890rec/s
3805010/516Mi 	|10000	|556s	|6855rec/s
12,434,402/1.73Gi 	|1000	|941s	v13270rec/s

### 4.3 测试总结

* 通过控制pageSize来控制写入速度，最佳为1000


## 5 约束限制
