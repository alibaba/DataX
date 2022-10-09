# DorisWriter 插件文档

## 1 快速介绍
DorisWriter支持将大批量数据写入Doris中。

## 2 实现原理
DorisWriter 通过Doris原生支持Stream load方式导入数据， DorisWriter会将`reader`读取的数据进行缓存在内存中，拼接成Json文本，然后批量导入至Doris。

## 3 功能说明

### 3.1 配置样例

这里是一份从Stream读取数据后导入至Doris的配置文件。

```
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "mysqlreader",
                    "parameter": {
                        "column": ["k1", "k2", "k3"],
                        "connection": [
                            {
                                "jdbcUrl": ["jdbc:mysql://127.0.0.1:3306/db1"],
                                "table": ["t1"]
                            }
                        ],
                        "username": "root",
                        "password": "",
                        "where": ""
                    }
                },
                "writer": {
                    "name": "doriswriter",
                    "parameter": {
                        "loadUrl": ["127.0.0.1:8030"],
                        "loadProps": {},
                        "database": "db1",
                        "column": ["k1", "k2", "k3"],
                        "username": "root",
                        "password": "",
                        "postSql": [],
                        "preSql": [],
                        "connection": [
                            "jdbcUrl":"jdbc:mysql://127.0.0.1:9030/demo",
                            "table":["xxx"],
                            "selectedDatabase":"xxxx"
                        ]
                    }
                }
            }
        ],
        "setting": {
            "speed": {
                "channel": "1"
            }
        }
    }
}
```

### 3.2 参数说明

* **jdbcUrl**

    - 描述：Doris 的 JDBC 连接串，用户执行 preSql 或 postSQL。
    - 必选：是
    - 默认值：无

* **loadUrl**

  - 描述：和 **beLoadUrl** 二选一。作为 Stream Load 的连接目标。格式为 "ip:port"。其中 IP 是 FE 节点 IP，port 是 FE 节点的 http_port。可以填写多个，doriswriter 将以轮询的方式访问。
  - 必选：是
  - 默认值：无

* **username**

    - 描述：访问Doris数据库的用户名
    - 必选：是
    - 默认值：无
    
* **password**
    
    - 描述：访问Doris数据库的密码
    - 必选：否
    - 默认值：空

* **database**

    - 描述：需要写入的Doris数据库名称。
    - 必选：是
    - 默认值：无
    
* **table**
    
    - 描述：需要写入的Doris表名称。
    - 必选：是
    - 默认值：无

* **column**

    - 描述：目的表**需要写入数据**的字段，这些字段将作为生成的 Json 数据的字段名。字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。
    - 必选：是
    - 默认值：否

* **preSql**

  - 描述：写入数据到目的表前，会先执行这里的标准语句。
  - 必选：否
  - 默认值：无

* **postSql**

  - 描述：写入数据到目的表后，会执行这里的标准语句。
  - 必选：否
  - 默认值：无


* **maxBatchRows**

  - 描述：每批次导入数据的最大行数。和 **maxBatchSize** 共同控制每批次的导入数量。每批次数据达到两个阈值之一，即开始导入这一批次的数据。
  - 必选：否
  - 默认值：500000

* **maxBatchSize**

  - 描述：每批次导入数据的最大数据量。和 **maxBatchRows** 共同控制每批次的导入数量。每批次数据达到两个阈值之一，即开始导入这一批次的数据。
  - 必选：否
  - 默认值：104857600

* **maxRetries**

  - 描述：每批次导入数据失败后的重试次数。
  - 必选：否
  - 默认值：0

* **labelPrefix**

  - 描述：每批次导入任务的 label 前缀。最终的 label 将有 `labelPrefix + UUID + 序号` 组成
  - 必选：否
  - 默认值：`datax_doris_writer_`

* **format**

  - 描述：StreamLoad数据的组装格式，支持csv和json格式。csv默认的行分隔符是\x01,列分隔符是\x02。
  - 必选：否
  - 默认值：csv
  
* **loadProps**

  - 描述：StreamLoad 的请求参数，详情参照StreamLoad介绍页面。
  - 必选：否
  - 默认值：无

* **connectTimeout**

  - 描述：StreamLoad单次请求的超时时间, 单位毫秒(ms)。
  - 必选：否
  - 默认值：-1
