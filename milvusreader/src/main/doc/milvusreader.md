### Datax MilvusReader
#### 1 快速介绍

MilvusReader 插件利用 Milvus 的java客户端MilvusClient进行Milvus的读操作。

#### 2 实现原理

MilvusReader通过Datax框架从Milvus读取数据，通过主控的JOB程序按照指定的规则对Milvus中的数据进行分片，并行读取，然后将Milvus支持的类型通过逐一判断转换成Datax支持的类型。

#### 3 功能说明
* 该示例从Milvus读一份Collection数据到另一个Milvus。
```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "milvusreader",
          "parameter": {
            "uri": "https://****.aws-us-west-2.vectordb.zillizcloud.com:19532",
            "token": "*****",
            "collection": "medium_articles",
            "batchSize": 10
          }
        },
        "writer": {
          "name": "milvuswriter",
          "parameter": {
            "uri": "https://*****.aws-us-west-2.vectordb.zillizcloud.com:19530",
            "token": "*****",
            "collection": "medium_articles",
            "batchSize": 10,
            "column": [
              {
                "name": "id",
                "type": "Int64",
                "isPrimaryKey": true
              },
              {
                "name": "title_vector",
                "type": "FloatVector",
                "dimension": 768
              },
              {
                "name": "title",
                "type": "VarChar",
                "maxLength": 1000
              },
              {
                "name": "link",
                "type": "VarChar",
                "maxLength": 1000
              },
              {
                "name": "reading_time",
                "type": "Int64"
              },
              {
                "name": "publication",
                "type": "VarChar",
                "maxLength": 1000
              },
              {
                "name": "claps",
                "type": "Int64"
              },
              {
                "name": "responses",
                "type": "Int64"
              }
            ]
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
	    
#### 4 参数说明

* uri： Milvus Cluster endpoint。【必填】
* token：Milvus的连接token。【必填】
* collection： 读取数据的collection。【必填】
* partition： 读取数据的partition。【选填】
* batchSize: 每次读取数据的行数【选填】

#### 5 类型转换

| DataX 内部类型| Milvus 数据类型     |
| -------- |-----------------|
| Long     | int             |
| Double   | double          |
| String   | string, varchar |
| Boolean  | bool            |

- 当前暂不支持读取dynamic schema的数据，及按partition读取

#### 6 性能报告
#### 7 测试报告
