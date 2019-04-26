
# OpenTSDBReader 插件文档

___


## 1 快速介绍

OpenTSDBReader 插件实现了从 OpenTSDB 读取数据。OpenTSDB 是主要由 Yahoo 维护的、可扩展的、分布式时序数据库，与阿里巴巴自研 TSDB 的关系与区别详见阿里云官网：《[相比 OpenTSDB 优势](https://help.aliyun.com/document_detail/113368.html)》



## 2 实现原理

在底层实现上，OpenTSDBReader 通过 HTTP 请求链接到 OpenTSDB 实例，利用 `/api/config` 接口获取到其底层存储 HBase 的连接信息，再利用 AsyncHBase 框架连接 HBase，通过 Scan 的方式将数据点扫描出来。整个同步的过程通过 metric 和时间段进行切分，即某个 metric 在某一个小时内的数据迁移，组合成一个迁移 Task。



## 3 功能说明

### 3.1 配置样例

* 配置一个从 OpenTSDB 数据库同步抽取数据到本地的作业：

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "opentsdbreader",
          "parameter": {
            "endpoint": "http://localhost:4242",
            "column": [
              "m"
            ],
            "beginDateTime": "2019-01-01 00:00:00",
            "endDateTime": "2019-01-01 03:00:00"
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "encoding": "UTF-8",
            "print": true
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



### 3.2 参数说明

* **name**
  * 描述：本插件的名称
  * 必选：是
  * 默认值：opentsdbreader

* **parameter**
  * **endpoint**
    * 描述：OpenTSDB 的 HTTP 连接地址
    * 必选：是
    * 格式：http://IP:Port
* 默认值：无
  
  * **column**
    * 描述：数据迁移任务需要迁移的 Metric 列表
    * 必选：是
  * 默认值：无
  
* **beginDateTime**
  * 描述：和 endDateTime 配合使用，用于指定哪个时间段内的数据点，需要被迁移
  * 必选：是
  * 格式：`yyyy-MM-dd HH:mm:ss`
  * 默认值：无
  * 注意：指定起止时间会自动忽略分钟和秒，转为整点时刻，例如 2019-4-18 的 [3:35, 4:55) 会被转为 [3:00, 4:00)
  
* **endDateTime**
  * 描述：和 beginDateTime 配合使用，用于指定哪个时间段内的数据点，需要被迁移
  * 必选：是
  * 格式：`yyyy-MM-dd HH:mm:ss`
  * 默认值：无
  * 注意：指定起止时间会自动忽略分钟和秒，转为整点时刻，例如 2019-4-18 的 [3:35, 4:55) 会被转为 [3:00, 4:00)




### 3.3 类型转换

| DataX 内部类型 | TSDB 数据类型                                                |
| -------------- | ------------------------------------------------------------ |
| String         | TSDB 数据点序列化字符串，包括 timestamp、metric、tags 和 value |





## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征
从 Metric、时间线、Value 和 采集周期 四个方面来描述：

##### metric

固定指定一个 metric 为 `m`。

##### tagkv

前四个 tagkv 全排列，形成 `10 * 20 * 100 * 100 = 2000000` 条时间线，最后 IP 对应 2000000 条时间线从 1 开始自增。

| **tag_k** | **tag_v**     |
| --------- | ------------- |
| zone      | z1~z10        |
| cluster   | c1~c20        |
| group     | g1~100        |
| app       | a1~a100       |
| ip        | ip1~ip2000000 |

##### value

度量值为 [1, 100] 区间内的随机值

##### interval

采集周期为 10 秒，持续摄入 3 小时，总数据量为 `3 * 60 * 60 / 10 * 2000000 = 2,160,000,000` 个数据点。



#### 4.1.2 机器参数

OpenTSDB Reader 机型:  64C256G

HBase 机型： 8C16G * 5


#### 4.1.3 DataX jvm 参数

"-Xms4096m -Xmx4096m"




### 4.2 测试报告


| 通道数| DataX 速度 (Rec/s) |DataX 流量 (MB/s)|
|--------| --------|--------|
|1| 215428 | 25.65 |
|2| 424994 | 50.60 |
|3| 603132 | 71.81 |






## 5 约束限制

### 5.1 需要确保与 OpenTSDB 底层存储的网络是连通的

具体缘由详见 6.1



### 5.2 如果存在某一个 Metric 下在一个小时范围内的数据量过大，可能需要通过 `-j` 参数调整 JVM 内存大小

考虑到下游 Writer 如果写入速度不及 OpenTSDB reader 的查询数据，可能会存在积压的情况，因此需要适当地调整 JVM 参数。以"从 OpenTSDB 数据库同步抽取数据到本地的作业"为例，启动命令如下：

```bash
 python datax/bin/datax.py opentsdb2stream.json -j "-Xms4096m -Xmx4096m"
```



### 5.3 指定起止时间会自动被转为整点时刻

指定起止时间会自动被转为整点时刻，例如 2019-4-18 的 `[3:35, 3:55)` 会被转为 `[3:00, 4:00)`



### 5.4 目前只支持兼容 OpenTSDB 2.3.x

其他版本暂不保证兼容





## 6 FAQ

***

**Q：为什么需要连接 OpenTSDB 的底层存储，为什么不直接使用 `/api/query` 查询获取数据点？**

A：因为通过 OpenTSDB 的 HTTP 接口（`/api/query`）来读取数据的话，经内部压测发现，在大数据量的情况下，会导致 OpenTSDB 的异步框架会报 CallBack 过多的问题；所以，采用了直连底层 HBase 存储，通过 Scan 的方式来扫描数据点，来避免这个问题。另外，还考虑到，可以通过指定 metric 和时间范围，可以顺序地 Scan HBase 表，提高查询效率。



