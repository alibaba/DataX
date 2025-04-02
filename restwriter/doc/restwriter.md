# DataX REstWriter 说明

------------

## 1 快速介绍

RestWriter提供了Restful接口写入数据的能力。RestWriter服务的用户主要在于DataX开发、测试同学。

## 2 功能与限制

RestWriter实现了向Restful接口同步数据的通用能力，RestWriter如下几个方面约定:

1. 通过请求题写入数据，支持post、put、patch3种http方法

2. 通过返回的http状态码判断写入成功或者失败

3. 支持失败重试与限流

我们不能做到：

1. 不支持嵌套对象。

## 3 功能说明

### 3.1 配置样例

```json
{
  "setting": {},
  "job": {
    "setting": {
      "speed": {
        "channel": 2
      }
    },
    "content": [
      {
        "reader": {
          "name": "txtfilereader",
          "parameter": {
            "path": [
              "/home/haiwei.luo/case00/data"
            ],
            "encoding": "UTF-8",
            "column": [
              {
                "index": 0,
                "type": "long"
              },
              {
                "index": 1,
                "type": "boolean"
              },
              {
                "index": 2,
                "type": "double"
              },
              {
                "index": 3,
                "type": "string"
              },
              {
                "index": 4,
                "type": "date",
                "format": "yyyy.MM.dd"
              }
            ],
            "fieldDelimiter": ","
          }
        },
        "writer": {
          "name": "restwriter",
          "parameter": {
            "url": "http://localhost:8080/echo",
            "method": "post",
            "ssl": false,
            "headers": {
              "aaa": "bbbb"
            },
            "query": {
              "test": "test"
            },
            "maxRetries": 3,
            "batch": false,
            "batchSize": 1000,
            "fields": [
              {
                "name": "id"
              },
              {
                "name": "jobGroup"
              },
              {
                "name": "jobId"
              },
              {
                "name": "executorAddress"
              },
              {
                "name": "executorHandler"
              },
              {
                "name": "executorParam"
              },
              {
                "name": "executorShardingParam"
              },
              {
                "name": "executorFailRetryCount"
              },
              {
                "name": "triggerTime",
                "type": "java.time.LocalDateTime"
              },
              {
                "name": "triggerCode"
              },
              {
                "name": "triggerMsg"
              },
              {
                "name": "handleTime",
                "type": "java.time.LocalDateTime"
              },
              {
                "name": "handleCode"
              },
              {
                "name": "handleMsg"
              },
              {
                "name": "alarmStatus"
              }
            ],
            "print": true,
            "failFast": false,
            "rate-per-task": 10
          }
        }
      }
    ]
  }
}
```

### 3.2 参数说明

* **url**

    * 描述：restful API URL，经过转义后的URL，RestWriter不负责转义特殊字符，比如空格等。 <br />

    * 必选：是 <br />

    * 默认值：无 <br />

* **method**

    * 描述：http method <br />

    * 必选：是 <br />

    * 默认值：无 <br />

* **ssl**

    * 描述：restful api是https/http，如果在url中给出protocol，则以url中为准<br />

    * 必选：否 <br />

    * 默认值：false <br />

* **headers**

    * 描述：http请求头 <br />

    * 必选：否 <br />

    * 默认值：无 <br />

* **query**

    * 描述：查询参数。 <br />

    * 必选：否 <br />

    * 默认值：无 <br />

* **maxRetries**

    * 描述：最大失败重试次数。<br />

    * 必选：否 <br />

    * 默认值：3 <br />


* **batch**

    * 描述：是否批量处理<br />

    * 必选：否 <br />

    * 默认值：false <br />

* **batchSize**

    * 描述：批量处理最大条数<br />

    * 必选：否 <br />

    * 默认值：100 <br />

* **fields**

    * 描述字段信息<br />

    * 必选：是 <br />

    * 默认值：无 <br />

* **print**

    * 描述：是否打印debug信息。<br />

    * 必选：否 <br />

    * 默认值：false <br />

### 3.3 类型转换


## 4 性能报告

## 5 约束限制

略

## 6 FAQ

略


