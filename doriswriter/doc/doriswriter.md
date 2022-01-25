# DorisWriter 插件文档

## 1 快速介绍
Apache Doris是一个现代化的MPP分析型数据库产品。仅需亚秒级响应时间即可获得查询结果，有效地支持实时数据分析。Apache Doris的分布式架构非常简洁，易于运维，并且可以支持10PB以上的超大数据集。

Apache Doris可以满足多种数据分析需求，例如固定历史报表，实时数据分析，交互式数据分析和探索式数据分析等。令您的数据分析工作更加简单高效！

DorisWriter是一个支持将大批量数据写入Doris中的一个插件，可以结合Datax其他读取数据的插件结合使用完成数据的整合。

Doris是完全支持Mysql协议的，所以如果你需要读取Doris的数据，可以使用mysqlreader插件完成，这里不单独提供Doris数据读取插件。

## 2.支持版本

DorisWriter目前支持的Doris版本如下：

| Doris版本号          | 说明                                            |
| -------------------- | ----------------------------------------------- |
| Apahce Doris 0.13.0  | 包括百度发行版palo 0.13.15                      |
| Apache Doris 0.14.0  | 包括百度发行版palo 0.14.7、0.14.12.1、0.14.13.1 |
| Apache Doris 0.15.0  | 包括百度发行版palo 0.15.1 RC09                  |
| Apahce Doris后续版本 |                                                 |

大家在使用过程中如果遇到什么问题，可以通过邮件或者在Doris的[Issues · apache/incubator-doris](https://github.com/apache/incubator-doris/issues)上提问，我们会及时解决，或者可以给Doris的开发组邮箱发送邮件：dev@doris.apache.org，我们也会及时查看回复及修复。

## 3 实现原理
DorisWriter 通过Doris原生支持Stream load方式导入数据， DorisWriter会将`reader`读取的数据进行缓存在内存中，拼接成Json文本，然后批量导入至Doris。

Stream load 是一个同步的导入方式，用户通过发送 HTTP 协议发送请求将本地文件或数据流导入到 Doris 中。Stream load 同步执行导入并返回导入结果。用户可直接通过请求的返回体判断本次导入是否成功。

Stream load 主要适用于导入本地文件，或通过程序导入数据流中的数据

Stream Load的数据导入流程如下：

```text
                         ^      +
                         |      |
                         |      | 1A. User submit load to FE
                         |      |
                         |   +--v-----------+
                         |   | FE           |
5. Return result to user |   +--+-----------+
                         |      |
                         |      | 2. Redirect to BE
                         |      |
                         |   +--v-----------+
                         +---+Coordinator BE| 1B. User submit load to BE
                             +-+-----+----+-+
                               |     |    |
                         +-----+     |    +-----+
                         |           |          | 3. Distrbute data
                         |           |          |
                       +-v-+       +-v-+      +-v-+
                       |BE |       |BE |      |BE |
                       +---+       +---+      +---+
```

Stream load 中，Doris 会选定一个节点作为 Coordinator 节点。该节点负责接数据并分发数据到其他数据节点。

用户通过 HTTP 协议提交导入命令。如果提交到 FE，则 FE 会通过 HTTP redirect 指令将请求转发给某一个 BE。用户也可以直接提交导入命令给某一指定 BE。

导入的最终结果由 Coordinator BE 返回给用户。

## 4 功能说明

### 4.1 配置样例

这里是一份从Stream读取数据后导入至Doris的配置文件。

```
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0
            }
        },
        "content": [
            {
                "reader": {
                    "name": "streamreader",
                    "parameter": {
                        "column": [
                            {
                                "value": "皮蛋1",
                                "type": "string"
                            },
                            {
                                "value": "皮蛋2",
                                "type": "string"
                            },
                            {
                                "value": "111",
                                "type": "long"
                            },
                            {
                                "value": "222",
                                "type": "long"
                            }
                        ],
                        "sliceRecordCount": 100
                    }
                },
                "writer": {
                    "name": "doriswriter",
                    "parameter": {
                        "feLoadUrl": ["127.0.0.1:8030", "127.0.0.2:8030", "127.0.0.3:8030"],
                        "beLoadUrl": ["192.168.10.1:8040", "192.168.10.2:8040", "192.168.10.3:8040"],
                        "jdbcUrl": "jdbc:mysql://127.0.0.1:9030/",
                        "database": "db1",
                        "table": "t1",
                        "column": ["k1", "k2", "v1", "v2"],
                        "username": "root",
                        "password": "",
                        "postSql": [],
                        "preSql": [],
                        "loadProps": {
                        },
                        "maxBatchRows" : 500000,
                        "maxBatchByteSize" : 104857600,
                        "labelPrefix": "my_prefix",
                        "lineDelimiter": "\n"
                    }
                }
            }
        ]
    }
}
```

### 4.2 参数说明

* **jdbcUrl**

    - 描述：Doris 的 JDBC 连接串，用户执行 preSql 或 postSQL。
    - 必选：是
    - 默认值：无

* **feLoadUrl**

  - 描述：和 **beLoadUrl** 二选一。作为 Stream Load 的连接目标。格式为 "ip:port"。其中 IP 是 FE 节点 IP，port 是 FE 节点的 http_port。可以填写多个，doriswriter 将以轮询的方式访问。
  - 必选：否
  - 默认值：无

* **beLoadUrl**

  - 描述：和 **feLoadUrl** 二选一。作为 Stream Load 的连接目标。格式为 "ip:port"。其中 IP 是 BE 节点 IP，port 是 BE 节点的 webserver_port。可以填写多个，doriswriter 将以轮询的方式访问。
  - 必选：否
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
  - 描述：每批次导入数据的最大行数。和 **maxBatchByteSize** 共同控制每批次的导入数量。每批次数据达到两个阈值之一，即开始导入这一批次的数据。
  - 必选：否
  - 默认值：500000
* **maxBatchByteSize**
  - 描述：每批次导入数据的最大数据量。和 ** maxBatchRows** 共同控制每批次的导入数量。每批次数据达到两个阈值之一，即开始导入这一批次的数据。
  - 必选：否
  - 默认值：104857600
* **labelPrefix**
  - 描述：每批次导入任务的 label 前缀。最终的 label 将有 `labelPrefix + UUID + 序号` 组成
  - 必选：否
  - 默认值：`datax_doris_writer_`
* **lineDelimiter**
  - 描述：每批次数据包含多行，每行为 Json 格式，每行的的分隔符即为 lineDelimiter。支持多个字节, 例如'\x02\x03'。
  - 必选：否
  - 默认值：`\n`
* **loadProps**
  - 描述：StreamLoad 的请求参数，详情参照StreamLoad介绍页面。
  - 必选：否
  - 默认值：无
* **connectTimeout**

  - 描述：StreamLoad单次请求的超时时间, 单位毫秒(ms)。
  - 必选：否
  - 默认值：-1

### 4.3 doriswriter插件的约束与限制

DorisWriter是借助于Apache Doris提供的Stream Load方式来实现数据导入，为了避免频繁的数据插入引发Doris写失败，建议批到的方式，具体参照[常见报错 | Apache Doris](https://doris.apache.org/zh-CN/faq/error.html#e3-tablet-writer-write-failed-tablet-id-27306172-txn-id-28573520-err-235-or-215-or-238)，建议将参数列表中的下列参数设大，下面给出建议值：

1. maxBatchRows：10000，表示每10000条提交导入一次，如果你的数据量没那么可以适当调小
2. maxBatchByteSize：这个参数表示你每个批到导入数据量大的大小，具体值=maxBatchRows * 单条记录的大小，如果一个批次导入的数据量大小超过这个值将被阻塞导入，导入数据格式是JSON格式所以这个值可以适当放大，通过上面的导入记录数来控制每个批次导入的数据量就可以了
3. column：这个要和你在Doris里建表的字段顺序一致。

## 4.性能测试

下面是通过读取Mysql数据表的数据，插入到Doris进行的性能测试结果，仅供参考

测试是单机测试，Mysql 8.0.26，Doris 0.15 （单机），mysql和Doris部署在同一台服务器上，服务器配置：4核 16 GiB

```
2022-01-25 23:32:53.638 [job-0] INFO  JobContainer - PerfTrace not enable!
2022-01-25 23:32:53.638 [job-0] INFO  StandAloneJobContainerCommunicator - Total 2000000 records, 80888896 bytes | Speed 3.86MB/s, 100000 records/s | Error 0 records, 0 bytes |  All Task WaitWriterTime 14.270s |  All Task WaitReaderTime 0.147s | Percentage 100.00%
2022-01-25 23:32:53.639 [job-0] INFO  JobContainer -
任务启动时刻                    : 2022-01-25 23:32:33
任务结束时刻                    : 2022-01-25 23:32:53
任务总计耗时                    :                 20s
任务平均流量                    :            3.86MB/s
记录写入速度                    :         100000rec/s
读出记录总数                    :             2000000
读写失败总数                    :                   0
```

