### Datax MongoDBWriter

#### 1 快速介绍

KafkaWriter 插件利用java kafka-clients创建消息生产者Producer<String, String>
进行Kafka消息发送操作。通过配置keys定义数据字段名，将消息组装成json格式，使用lz4进行压缩。

#### 2 实现原理

KafkaWriter 插件利用java kafka-clients创建消息生产者Producer<String, String>进行Kafka消息发送操作

#### 3 功能说明

* 该示例从Mysql读一份数据发送到Kafka的配置。

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "username": "root",
            "password": "reformer@123",
            "splitPk": "id",
            "connection": [
              {
                "querySql": [
                  "SELECT id, name from student_test"
                ],
                "jdbcUrl": [
                  "jdbc:mysql://192.168.11.100:7006/lzc_test"
                ]
              }
            ],
            "where": "1 = 1"
          }
        },
        "writer": {
          "name": "kafkawriter",
          "parameter": {
            "bootstrapServers": "kafka01:9380,kafka02:9381,kafka03:9382",
            "topic": "test_lu",
            "keys": [
              "id",
              "name"
            ]
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 3
      }
    }
  }
}

```

#### 4 参数说明

* bootstrapServers： kafka中心bootstrapServers地址。【必填】【默认值：无】
* topic：发送消息的主题。【必填】【默认值：无】
* keys： 组装json数据的属性字段。【必填】【默认值：无】

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
