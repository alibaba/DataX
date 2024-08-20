### Datax KafkaWriter

#### 1 快速介绍

KafkaWriter 插件利用java kafka-clients创建消息生产者Producer<String, String>
进行Kafka消息发送操作。通过配置keys定义数据字段名，将消息组装成json格式，使用lz4进行压缩。

#### 2 实现原理

KafkaWriter 插件利用java kafka-clients创建消息生产者Producer<String, String>进行Kafka消息发送操作

#### 3 功能说明

* 该示例从streamreader生成10行数据发送到Kafka的配置。

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "sliceRecordCount": 10,
            "column": [
              {
                "type": "long",
                "value": "10"
              },
              {
                "type": "string",
                "value": "hello，你好，世界-DataX"
              },
              {
                "type": "date",
                "value": "2014-07-07 00:00:00"
              }
            ]
          }
        },
        "writer": {
          "name": "kafkawriter",
          "parameter": {
            "bootstrapServers": "10.0.0.11:9092",
            "topic": "ods_stream_test",
            "keyIndex": 0,
            "column": [
              "id",
              "content",
              "date"
            ],
            "props": {
              "acks": "1"
            }
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

* bootstrapServers： kafka中心bootstrapServers地址。【必填】【默认值：无】
* topic：发送消息的主题。【必填】【默认值：无】
* column： 组装json数据的属性字段。【必填】【默认值：无】
* props： Kafka自定义参数。【选填】【默认值：无】

#### 5 类型转换

| DataX 内部类型 | Kafka 数据类型 |
|------------|------------|
| Long       | String     |
| Double     | String     |
| String     | String     |
| Date       | String     |
| Boolean    | String     |
| Bytes      | String     |

#### 6 性能报告

#### 7 测试报告