### Datax KuduReader
#### 1 快速介绍

KuduReader 插件利用 Kudu 的java客户端KuduClient进行Kudu的读操作。

#### 2 实现原理

KuduReader通过Datax框架从Kudu并行的读取数据，通过主控的JOB程序按照指定的规则对Kudu中的数据进行分片，并行读取，然后将Kudu支持的类型通过逐一判断转换成Datax支持的类型。

#### 3 功能说明
* 配置一个从Kudu同步抽取数据到本地的作业:

```
{
    "job": {
        "setting": {
            "speed": {
                 "channel": 3
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0.02
            }
        },
        "content": [
            {
                "reader": {
                    "name": "kudureader",
                    "parameter": {
                        "masterAddresses": "127.0.0.1:7051",
                        "tableName": "test"
                    }
                },
               "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print":true
                    }
                }
            }
        ]
    }
}

```

#### 4 参数说明

* masterAddresses： Kudu Master集群RPC地址。【必填】
* tableName：Kudu的表名。【选填】

#### 5 类型转换

| DataX 内部类型| Kudu 数据类型    |
| -------- | -----  |
| Long     | byte, short, int, long |
| Double   | float, double |
| String   | string |
| Date     | timestamp  |
| Boolean  | boolean |
| Bytes    | binary |


#### 6 性能报告
#### 7 测试报告