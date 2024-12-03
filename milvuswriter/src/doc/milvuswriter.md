### Datax MilvusWriter插件
#### 1 快速介绍

MilvusWriter 插件利用 Milvus 的java客户端MilvusClient进行Milvus的写操作。

#### 2 实现原理

MilvusWriter通过Datax框架向Milvus写入数据，通过主控的JOB程序按照指定的规则向Milvus写入，然后将Datax的类型通过逐一判断转换成Milvus支持的类型。

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
            "schemaCreateMode": 0,
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
* schemaCreateMode: Integer, schema创建模式, 默认为createWhenTableNotExit [0(createWhenTableNotExit),1(exception)]【选填】
* enableDyanmicSchema: 是否启用动态schema, 默认为true【选填】
* column: 写入的字段信息【必填】
  * name: 字段名【必填】
  * type: 字段类型[Int8, Int16, Int32, Int64, Float, Double, VarChar, FloatVector, JSON, Array]【必填】
  * isPrimaryKey: 是否为主键【选填】
  * isPartitionKey: 是否为分区键【选填】
  * dimension: FloatVector类型的维度【选填】
  * maxLength: VarChar类型的最大长度【选填】
  * elementType: Array类型的元素类型【选填】
  * maxcapacity: Array类型的最大容量【选填】
#### 5 类型转换

| DataX 内部类型| Milvus 数据类型     |
| -------- |-----------------|
| Long     | int             |
| Double   | double          |
| String   | string, varchar |
| Boolean  | bool            |

- 当前暂不支持写入dynamic schema的数据，及按partition写入

#### 6 性能报告
#### 7 测试报告
