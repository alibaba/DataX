# HBase11xsqlwriter插件文档

## 1. 快速介绍

HBase11xsqlwriter实现了向hbase中的SQL表(phoenix)批量导入数据的功能。Phoenix因为对rowkey做了数据编码，所以，直接使用HBaseAPI进行写入会面临手工数据转换的问题，麻烦且易错。本插件提供了单间的SQL表的数据导入方式。

在底层实现上，通过Phoenix的JDBC驱动，执行UPSERT语句向hbase写入数据。

### 1.1 支持的功能

* 支持带索引的表的数据导入，可以同步更新所有的索引表


### 1.2 限制

* 仅支持1.x系列的hbase
* 仅支持通过phoenix创建的表，不支持原生HBase表
* 不支持带时间戳的数据导入

## 2. 实现原理

通过Phoenix的JDBC驱动，执行UPSERT语句向表中批量写入数据。因为使用上层接口，所以，可以同步更新索引表。

## 3. 配置说明

### 3.1 配置样例

```json
{
  "job": {
    "entry": {
      "jvm": "-Xms2048m -Xmx2048m"
    },
    "content": [
      {
        "reader": {
          "name": "txtfilereader",
          "parameter": {
            "path": "/Users/shf/workplace/datax_test/hbase11xsqlwriter/txt/normal.txt",
            "charset": "UTF-8",
            "column": [
              {
                "index": 0,
                "type": "String"
              },
              {
                "index": 1,
                "type": "string"
              },
              {
                "index": 2,
                "type": "string"
              },
              {
                "index": 3,
                "type": "string"
              }
            ],
            "fieldDelimiter": ","
          }
        },
        "writer": {
          "name": "hbase11xsqlwriter",
          "parameter": {
            "batchSize": "256",
            "column": [
              "UID",
              "TS",
              "EVENTID",
              "CONTENT"
            ],
            "hbaseConfig": {
              "hbase.zookeeper.quorum": "目标hbase集群的ZK服务器地址，向PE咨询",
              "zookeeper.znode.parent": "目标hbase集群的znode，向PE咨询"
            },
            "nullMode": "skip",
            "table": "目标hbase表名，大小写有关"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 5
      }
    }
  }
}
```


### 3.2 参数说明

* **name**

   * 描述：插件名字，必须是`hbase11xsqlwriter`
   * 必选：是
   * 默认值：无

* **table**

   * 描述：要导入的表名，大小写敏感，通常phoenix表都是**大写**表名
   * 必选：是
   * 默认值：无

* **column**

   * 描述：列名，大小写敏感，通常phoenix的列名都是**大写**。
       * 需要注意列的顺序，必须与reader输出的列的顺序一一对应。
       * 不需要填写数据类型，会自动从phoenix获取列的元数据
   * 必选：是
   * 默认值：无

* **hbaseConfig**

   * 描述：hbase集群地址，zk为必填项，格式：ip1,ip2,ip3，注意，多个IP之间使用英文的逗号分隔。znode是可选的，默认值是/hbase
   * 必选：是
   * 默认值：无

* **batchSize**

   * 描述：批量写入的最大行数
   * 必选：否
   * 默认值：256

* **nullMode**

   * 描述：读取到的列值为null时，如何处理。目前有两种方式：
      * skip：跳过这一列，即不插入这一列(如果该行的这一列之前已经存在，则会被删除)
      * empty：插入空值，值类型的空值是0，varchar的空值是空字符串
   * 必选：否
   * 默认值：skip

## 4. 性能报告

无

## 5. 约束限制

writer中的列的定义顺序必须与reader的列顺序匹配。reader中的列顺序定义了输出的每一行中，列的组织顺序。而writer的列顺序，定义的是在收到的数据中，writer期待的列的顺序。例如：

reader的列顺序是： c1, c2, c3, c4

writer的列顺序是： x1, x2, x3, x4

则reader输出的列c1就会赋值给writer的列x1。如果writer的列顺序是x1, x2, x4, x3，则c3会赋值给x4，c4会赋值给x3.


## 6. FAQ

1. 并发开多少合适？速度慢时增加并发有用吗？
   数据导入进程默认JVM的堆大小是2GB，并发(channel数)是通过多线程实现的，开过多的线程有时并不能提高导入速度，反而可能因为过于频繁的GC导致性能下降。一般建议并发数(channel)为5-10.

2. batchSize设置多少比较合适？
默认是256，但应根据每行的大小来计算最合适的batchSize。通常一次操作的数据量在2MB-4MB左右，用这个值除以行大小，即可得到batchSize。




