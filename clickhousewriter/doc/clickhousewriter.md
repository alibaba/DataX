# DataX ClickHouseWriter


---

## 1 快速介绍

数据导入clickhousewriter的插件

## 2 实现原理

使用clickhousewriter的官方jdbc接口， 批量把从reader读入的数据写入ClickHouse

## 3 功能说明

### 3.1 配置样例

#### job.json

```
{
  "job": {
    "setting": {
        "speed": {
            "channel": 2
        }
    },
    "content": [
      {
        "reader": {
          ...
        },
        "writer": {
            "name": "clickhousewriter",
            "parameter": {
                "username": "default",
                "password": "zifei123",
                "column":["belonger","belonger_type","bs_flag","id","income_asset_code","income_fee","insert_time","logName","order_no","pair_id","price","quantity","trade_date","trade_no","trade_time","update_time"],
                "connection": [
                    {
                        "jdbcUrl": "jdbc:clickhouse://127.0.0.1:8123/default",
                        "table":["XXX"]
                    }
                ]
            }
        }
      }
    ]
  }
}
```

#### 3.2 参数说明

* jdbcUrl
 * 描述：ClickHouse的连接地址，目前支持多数据源并行导入，支持随机负载均衡,其格式为:jdbc:clickhouse://ip1:8123,ip2:8123/database
 * 必选：是
 * 默认值：无

* username
 * 描述：导入数据源的用户名
 * 必选：是
 * 默认值：空

* password
 * 描述：导入数据源的密码
 * 必选：是
 * 默认值：空

* batchSize
 * 描述：每次批量数据的条数
 * 必选：否
 * 默认值：2048

* trySize
 * 描述：失败后重试的次数
 * 必选：否
 * 默认值：30

* column
 * 描述：elasticsearch所支持的字段类型，样例中包含了全部
 * 必选：是



## 4 性能报告

### 4.1 环境准备

* 总数据量 1.2亿条数据, 每条1.2kb
* 1个replica,单机模式

#### 4.1.1 数据特征

建表语句：

```sql
CREATE TABLE default.t_match_record (
       id                      UInt32,
       trade_no                UInt32,
       order_no                UInt32,
       pair_id                 String,
       belonger                String,
       login_name              String,
       belonger_type           String,
       trade_date              Date,
       trade_time              DateTime,
       bs_flag                 String,
       price                   Decimal64(8),
       quantity                UInt32,
       income_asset_code       String,
       income_fee              Decimal64(8),
       insert_time             DateTime,
       update_time             DateTime
)Engine=MergeTree(trade_date,(belonger,pair_id,trade_no,trade_date),8192);
```

插入记录类似于：

```
INSERT INTO `default`.`t_match_record`(`id`, `trade_no`, `order_no`, `pair_id`, `belonger`, `login_name`, `belonger_type`, `trade_date`, `trade_time`, `bs_flag`, `price`, `quantity`, `income_asset_code`, `income_fee`, `update_time`, `insert_time`) VALUES (141135300, 116615441, 115754819, 'ETH-USDT', '2357246974', '131****4807', '0', '2019-04-21', '2019-04-21 00:34:19', 'B', 113.02000000, 0, 'C10001', 0.00001110, '2018-12-21 00:35:00', '2018-12-21 00:35:00');
INSERT INTO `default`.`t_match_record`(`id`, `trade_no`, `order_no`, `pair_id`, `belonger`, `login_name`, `belonger_type`, `trade_date`, `trade_time`, `bs_flag`, `price`, `quantity`, `income_asset_code`, `income_fee`, `update_time`, `insert_time`) VALUES (141135299, 116615440, 115754793, 'ETH-USDT', '2357246974', '131****4807', '0', '2019-04-21', '2019-04-21 00:34:19', 'S', 113.02000000, 0, 'C10002', 0.00037297, '2018-12-21 00:35:00', '2018-12-21 00:35:00');

```

#### 4.1.2 机器参数
虚拟机配置如下
1. cpu: 2物理2逻辑  Intel(R) Core(TM) i5-8600 CPU @ 3.10GHz
2. mem: 2G
3. net: 千兆双网卡

#### 4.1.3 DataX jvm 参数

-Xms1024m -Xmx1024m -XX:+HeapDumpOnOutOfMemoryError

### 4.2 测试报告

| 通道数|  批量提交行数| DataX速度(Rec/s)|DataX流量(MB/s)|
|--------|--------| --------|--------|
| 1| 8192| 115379| 13.09|
| 2| 2048| 144224| 16.37|
| 2| 4096| 151815| 17.23|
| 2| 8192| 162506| 18.44|
| 4| 2048| 151815| 17.23|
| 4| 4096| 172208| 19.54|
| 4| 8192| 202420| 22.97|


### 4.3 测试总结

* 最好的结果是1-2通道，每次传8192（对当前笔者测试坏境配置而言，瓶颈在CPU上），如果单条数据很大， 请适当减少批量数，防止oom
* 通过升级硬件，单机写入300K/S不是问题，甚至500K/S，而且ClickHouse也是分布式的，多设置几个分片就可以水平扩展，此时还可以并行写入
* 当通道为4，批量提交为8192时，笔者测试机器已压榨到极限：物理机CPU100%，磁盘占用100%，网卡流量峰值为360Mbps。

### 4.4 导入建议
* 数据应该以尽量大的batch进行写入，如每次写入100,000行，根据机器性能，尝试增加通道数
* 数据最好跟ClickHouse分区Key分组排序，这样有更好的插入性能