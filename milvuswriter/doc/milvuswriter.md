# DataX milvuswriter


---


## 1 快速介绍

milvuswriter 插件实现了写入数据到 milvus集合的功能; 面向ETL开发工程师，使用 milvuswriter 从数仓导入数据到 milvus, 同时 milvuswriter 亦可以作为数据迁移工具为DBA等用户提供服务。


## 2 实现原理

milvuswriter 通过 DataX 框架获取 Reader 生成的协议数据，通过 `upsert/insert `方式写入数据到milvus, 并通过batchSize累积的方式进行数据提交。
<br />

    注意：upsert写入方式（推荐）: 在非autid表场景下根据主键更新 Collection 中的某个 Entity；autid表场景下会将 Entity 中的主键替换为自动生成的主键并插入数据。
         insert写入方式: 多用于autid表插入数据milvus自动生成主键， 非autoid表下使用insert会导致数据重复。


## 3 功能说明

### 3.1 配置样例

* 这里提供一份从内存产生数据导入到 milvus的配置样例。

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column" : [
              {
                "value": 1,
                "type": "long"
              },
              {
                "value": "[1.1,1.2,1.3]",
                "type": "string"
              },
              {
                "value": 100,
                "type": "long"
              },
              {
                "value": 200,
                "type": "long"
              },
              {
                "value": 300,
                "type": "long"
              },
              {
                "value": 3.14159,
                "type": "double"
              },
              {
                "value": 3.1415926,
                "type": "double"
              },
              {
                "value": "testvarcharvalue",
                "type": "string"
              },
              {
                "value": true,
                "type": "bool"
              },
              {
                "value": "[1.123,1.2456,1.3789]",
                "type": "string"
              },
              {
                "value": "[2.123,2.2456,2.3789]",
                "type": "string"
              },
              {
                "value": "12345678",
                "type": "string"
              },
              {
                "value": "{\"a\":1,\"b\":2,\"c\":3}",
                "type": "string"
              },
              {
                "value": "[1,2,3,4]",
                "type": "string"
              }
            ],
            "sliceRecordCount": 1
          }
        },
        "writer": {
          "parameter": {
            "schemaCreateMode": "createIfNotExist",
            "connectTimeoutMs": 60000,
            "writeMode": "upsert",
            "collection": "demo01",
            "type": "milvus",
            "token": "xxxxxxx",
            "endpoint": "https://xxxxxxxx.com:443",
            "batchSize": 1024,
            "column": [
              {
                "name": "id",
                "type": "Int64",
                "primaryKey": "true"
              },
              {
                "name": "floatvector",
                "type": "FloatVector",
                "dimension": "3"
              },
              {
                "name": "int8col",
                "type": "Int8"
              },
              {
                "name": "int16col",
                "type": "Int16"
              },
              {
                "name": "int32col",
                "type": "Int32"
              },
              {
                "name": "floatcol",
                "type": "Float"
              },
              {
                "name": "doublecol",
                "type": "Double"
              },
              {
                "name": "varcharcol",
                "type": "VarChar"
              },
              {
                "name": "boolcol",
                "type": "Bool"
              },
              {
                "name": "bfloat16vectorcol",
                "type": "BFloat16Vector",
                "dimension": "3"
              },
              {
                "name": "float16vectorcol",
                "type": "Float16Vector",
                "dimension": "3"
              },
              {
                "name": "binaryvectorcol",
                "type": "BinaryVector",
                "dimension": "64"
              },
              {
                "name": "jsoncol",
                "type": "JSON"
              },
              {
                "name": "intarraycol",
                "maxCapacity": "8",
                "type": "Array",
                "elementType": "Int32"
              }
            ]
          },
          "name": "milvuswriter"
        }
      }
    ],
    "setting": {
      "errorLimit": {
        "record": "0"
      },
      "speed": {
        "concurrent": 2,
        "channel": 2
      }
    }
  }
}

```


### 3.2 参数说明

* **endpoint**
    * 描述：milvus数据库的连接信息，包含地址和端口，例如https://xxxxxxxx.com:443

               注意：1、在一个数据库上只能配置一个 endpoint 值
                    2、一个milvus 写入任务仅能配置一个 endpoint
 	* 必选：是 <br />
	* 默认值：无 <br />
* *schemaCreateMode*
    * 描述： 集合创建的模式，同步时milvus集合不存在的处理方式， 根据配置的column属性进行创建
    * 取值
      * createIfNotExist： 如果集合不存在，则创建集合，如果集合存在，则不执行任何操作
      * ignore： 如果集合不存在，任务异常报错，如果集合存在，则不执行任何操作
      * recreate： 如果集合不存在，则创建集合，如果集合存在，则删除集合重建集合
    * 必选：否 <br />
    * 默认值：createIfNotExist <br />
* **connectTimeoutMs**
  * 描述：与milvus交互是客户端的连接超时时间，单位毫秒 <br />
  * 必选：否 <br />
  * 默认值：10000 <br />
* **token**
  * 描述：milvus实例认证的token秘钥，与username认证方式二选一配置 <br />
  * 必选：否 <br />
  * 默认值：无 <br />
* **username**
    * 描述：目的milvus数据库的用户名， 与token二选一配置 <br />
    * 必选：否 <br />
    * 默认值：无 <br />
* **password**
    * 描述：目的milvus数据库的密码 <br />
    * 必选：否 <br />
    * 默认值：无 <br />
* *writeMode*
  * 描述： 写入milvus集合的写入方式
  * 取值
    * upsert（推荐）: 在非autid表场景下根据主键更新 Collection 中的某个 Entity；autid表场景下会将 Entity 中的主键替换为自动生成的主键并插入数据。
    * insert: 多用于autid表插入数据milvus自动生成主键， 非autoid表下使用insert会导致数据重复。
  * 必选：是 <br />
  * 默认值：upsert <br />
* **collection**
    * 描述：目的集合名称。 只能配置一个milvus的集合名称。
    * 必选：是 <br />
    * 默认值：无 <br />
* **batchSize**
  * 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与milvus的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。<br />
  * 必选：否 <br />
  * 默认值：1024<br />

* **column**
  * 描述：目的集合需要写入数据的字段，字段内容用json格式描述，字段之间用英文逗号分隔。字段属性必填name、type, 其他属性在需要schemaCreateMode创建集合按需填入，例如:

          "column": [
                  {
                  "name": "id",
                  "type": "Int64",
                  "primaryKey": "true"
                  },
                  {
                  "name": "floatvector",
                  "type": "FloatVector",
                  "dimension": "3"
                  }]
    * 必选：是 <br />
    * 默认值：否 <br />
### 3.3 支持同步milvus字段类型
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Float,
    Double,
    String,
    VarChar,
    Array,
    JSON,
    BinaryVector,
    FloatVector,
    Float16Vector,
    BFloat16Vector,
    SparseFloatVector

