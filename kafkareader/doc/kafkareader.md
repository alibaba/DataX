# DataX kafkaReader 插件文档


------------

## 1 快速介绍

kafkaReader插件实现了消费topic数据。在底层实现上，kafkaReader通过参数连接kafka，并发消费数据，支持两种模式：批量读取（batch），消费任务启动之前的数据。流式读取（stream)，实时消费数据。支持数据格式：json格式，分隔符格式。

## 2 功能与限制
kafka到hdfs推荐用批量读取(batch)，调度设置定时表达式，每次抽取启动时刻之前的数据；kafka到其他rdbms可以用流式读取(stream)。

## 3 功能说明


### 3.1 配置样例

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 2
      },
      "errorLimit": {
        "record": 100
      }
    },
    "content": [
      {
        "reader": {
          "name": "kafkareader",
          "parameter": {
            "type": "batch",
            "topic": "datalinktestjson",
            "bootstrapServers": "nn205.uat.company.cn:9092,nn206.uat.company.cn:9092,slave207.uat.company.cn:9092,slave208.uat.company.cn:9092,slave209.uat.company.cn:9092",
            "groupId": "datalinktest-groupid-001",
            "parsingRules": "json",
            "writerOrder": "1,2,3,4,5,6,7,8,9",
            "kafkaReaderColumnKey": "data[0].id,data[0].order_no,data[0].product_code,data[0].out_product_code,data[0].product_name,data[0].product_url,data[0].category_code,data[0].category_name,data[0].has_group_item",
            "kafkaConfig": {
              "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
              "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
            },
            "haveKerberos": true,
            "kerberosConfig": {
              "security.protocol": "SASL_PLAINTEXT",
              "sasl.kerberos.service.name": "kafka",
              "sasl.mechanism": "GSSAPI"
            }
          }
        },
        "writer": {
          "name": "mysqlwriter",
          "parameter": {
            "writeMode": "insert",
            "username": "***",
            "password": "***",
            "column": [
              "id",
              "relation_a",
              "relation_b",
              "relation_c",
              "type",
              "sec_type",
              "a_ext",
              "b_ext",
              "c_ext"
            ],
            "session": [
              "set session sql_mode='ANSI'"
            ],
            "connection": [
              {
                "table": [
                  "kafka_test_table"
                ],
                "jdbcUrl": "jdbc:mysql://10.0.68.230:3306/dxgroup001"
              }
            ]
          }
        }
      }
    ]
  }
}

```

### 3.2 参数说明

o	type
-	描述：kafka读类型，batch：批量读取，stream：流式读取
kafka到hdfs推荐用批量读取(batch)，调度设置定时表达式，每次抽取启动时刻之前的数据；kafka到其他rdbms可以用流式读取(stream)。
o	必选：是
o	所有选项：batch/stream
o	默认值：无
o	topic
-	描述：kafka的topic
-	必选：是
-	默认值：无
o	bootstrapServers
-	描述：kafka消费bootstrapServers
-	必选：是
-	默认值：无
o	groupId
-	描述：kafka消费groupId
-	必选：是
-	默认值：无
o	parsingRules
-	描述：读取文件类型
-	所有选项：json/split
-	必选：是
-	默认值：无
o	split
-	描述：分隔符，如果split格式需要分隔符
-	必选：否
-	默认值：，
o	writerOrder
-	描述：读取顺序
-	必选：是
-	默认值：无
o	kafkaReaderColumnKey
-	描述：读取数据信息
eg：json格式数据：
eg：
json格式数据：
{
    "data":[
        {
            "id":"22_20200722 17:09:06",
            "order_no":"926585288000641",
            "product_code":"837440",
            "out_product_code":"2024953427",
            "product_name":"立白精致衣物护理洗衣液",
            "product_url":"http://hotfile.company.cn/files/|cephdata|filecache|aaYS|aaYS|2019-08-25|1726037918423715840",
            "category_code":"103001",
            "category_name":"衣物洗涤",
            "has_group_item":"0"
        }
    ]
}

kafkaReaderColumnKey：
data[0].id,data[0].order_no,data[0].product_code,data[0].out_product_code,data[0].product_name,data[0].product_url,data[0].category_code,data[0].category_name,data[0].has_group_item
 
o	必选：是
o	默认值：无
o	kafkaConfig
-	描述：kafka自定义设置，Properties可设置参数都可以使用，（enable.auto.commit默认是false，具体提交offset原理见kafkareader设计文档）
"kafkaConfig": {
              "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
              "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
            }

o	必选：是
o	默认值：无
o	haveKerberos
-	描述：是否有Kerberos认证，默认false
例如如果用户配置true，则配置项kerberosConfig为必填。
o	必选：haveKerberos 为true必选
o	默认值：false
o	kerberosConfig
-	描述：Kerberos认证配置
"kerberosConfig": {
              "security.protocol": "SASL_PLAINTEXT",
              "sasl.kerberos.service.name": "kafka",
              "sasl.mechanism": "GSSAPI"
            }
o	必选：否
o	默认值：无


### 3.3 类型转换

kafkareader读取列信息在datax内部类型会转化为string



