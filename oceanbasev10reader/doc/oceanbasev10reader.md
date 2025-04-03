## 1 快速介绍
OceanbaseV10Reader插件实现了从Oceanbase V1.0读取数据。在底层实现上，该读取插件通过java client（jdbc）连接远程Oceanbase 1.0数据库，并执行相应的sql语句将数据从库中SELECT出来。

注意，oceanbasev10reader只适用于ob1.0及以后版本的reader。

## 2 实现原理
简而言之，Oceanbasev10Reader通过java client连接器连接到远程的Oceanbase数据库，并根据用户配置的信息生成查询SELECT SQL语句，然后发送到远程Oceanbase v1.0及更高版本数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。<br />对于用户配置Table、Column、Where的信息，OceanbaseV10Reader将其拼接为SQL语句发送到Oceanbase 数据库；对于用户配置querySql信息，Oceanbasev10Reader直接将其发送到Oceanbase数据库。
## 3 功能说明
### 3.1 配置样例

- 配置一个从Oceanbase数据库同步抽取数据到本地的作业:
```
{
    "job": {
        "setting": {
            "speed": {
            //设置传输速度，单位为byte/s，DataX运行会尽可能达到该速度但是不超过它.
                 "byte": 1048576
            }
            //出错限制
                "errorLimit": {
                //出错的record条数上限，当大于该值即报错。
                "record": 0,
                //出错的record百分比上限 1.0表示100%，0.02表示2%
                "percentage": 0.02
            }
        },
        "content": [
            {
                "reader": {
                    "name": "oceanbasev10reader",
                    "parameter": {
                        "where": "",
                        "timeout": 5,
                        "readBatchSize": 50000,
                        "column": [
                            "id"，"name"
                        ],
                        "connection": [
                            {
                                "jdbcUrl": ["||_dsc_ob10_dsc_||集群名:租户名||_dsc_ob10_dsc_||jdbc:mysql://obproxyIp:obproxyPort/dbName"],
                                "table": [
                                    "table"
                                ]
                            }
                        ]
                    }
                },
               "writer": {
                    //writer类型
                    "name": "streamwriter",
                    //是否打印内容
                    "parameter": {
                        "print":true,
                    }
                }
            }
        ]
    }
}
```
```
{
    "job": {
        "setting": {
            "speed": {
                "channel": 3
            },
            "errorLimit": {
                "record": 0
            }
        },
        "content": [
            {
                "reader": {
                    "name": "oceanbasev10reader",
                    "parameter": {
                        "where": "",
                        "timeout": 5,
                        "fetchSize": 500,
                        "column": [
                            "id",
                            "name"
                        ],
                        "splitPk": "pk",
                        "connection": [
                            {
                                "jdbcUrl": ["||_dsc_ob10_dsc_||集群名:租户名||_dsc_ob10_dsc_||jdbc:mysql://obproxyIp:obproxyPort/dbName"],
                                "table": [
                                    "table"
                                ]
                            }
                        ],
                        "username":"xxx",
                        "password":"xxx"
                    }
                },
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print": true
                    }
                }
            }
        ]
    }
}
```

- 配置一个自定义SQL的数据库同步任务到本地内容的作业：
```
{
    "job": {
        "setting": {
            "channel": 3
        },
        "content": [
            {
                "reader": {
                    "name": "oceanbasev10reader",
                    "parameter": {
                        "timeout": 5,
                        "fetchSize": 500,
                        "splitPk": "pk",
                        "connection": [
                            {
                                "jdbcUrl": ["||_dsc_ob10_dsc_||集群名:租户名||_dsc_ob10_dsc_||jdbc:mysql://obproxyIp:obproxyPort/dbName"],
                                "querySql": [
                                    "select db_id,on_line_flag from db_info where db_id < 10;"
                                ]
                            }
                        ],
                        "username":"xxx",
                        "password":"xxx"
                    }
                },
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print": false,
                        "encoding": "UTF-8"
                    }
                }
            }
        ]
    }
}
```
### 3.2 参数说明

- **jdbcUrl**
  - 描述：连接ob使用的jdbc url，支持两种格式：
    - ||_dsc_ob10_dsc_||集群名:租户名||_dsc_ob10_dsc_||jdbc:mysql://obproxyIp:obproxyPort/db
      - 此格式下username仅填写用户名本身，无需三段式写法
    - jdbc:mysql://ip:port/db
      - 此格式下username需要三段式写法
  - 必选：是
  - 默认值：无
- **table**
  - 描述：所选取的需要同步的表。使用JSON的数组描述，因此支持多张表同时抽取。当配置为多张表时，用户自己需保证多张表是同一schema结构，OceanbaseReader不予检查表是否同一逻辑表。注意，table必须包含在connection配置单元中。
  - 必选：是
  - 默认值：无
- **column**
  - 描述：所配置的表中需要同步的列名集合，使用JSON的数组描述字段信息。
- 支持列裁剪，即列可以挑选部分列进行导出。
```
支持列换序，即列可以不按照表schema信息进行导出，同时支持通配符*，在使用之前需仔细核对列信息。
```

- 必选：是
- 默认值：无
- **where**
  - 描述：筛选条件，OceanbaseReader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为gmt_create > $bizdate 。这里gmt_create不可以是索引字段，也不可以是联合索引的第一个字段<br />。<br />where条件可以有效地进行业务增量同步。如果不填写where语句，包括不提供where的key或者value，DataX均视作同步全量数据
  - 必选：否
  - 默认值：无
- **splitPk**
  - 描述：OBReader进行数据抽取时，如果指定splitPk，表示用户希望使用splitPk代表的字段进行数据分片，DataX因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能。
  - 推荐splitPk用户使用表主键，因为表主键通常情况下比较均匀，因此切分出来的分片也不容易出现数据热点。
  - 目前splitPk仅支持int数据切分，`不支持其他类型`。如果用户指定其他非支持类型将报错。<br />splitPk如果不填写，将视作用户不对单表进行切分，OBReader使用单通道同步全量数据。
  - 必选：否
  - 默认值：空
- **querySql**
  - 描述：在有些业务场景下，where这一配置项不足以描述所筛选的条件，用户可以通过该配置型来自定义筛选SQL。当用户配置了这一项之后，DataX系统就会忽略table，column这些配置型，直接使用这个配置项的内容对数据进行筛选
- `当用户配置querySql时，OceanbaseReader直接忽略table、column、where条件的配置`，querySql优先级大于table、column、where选项。
  - 必选：否
  - 默认值：无
- **timeout**
  - 描述：sql执行的超时时间 单位分钟
  - 必选：否
  - 默认值：5
- **username**
  - 描述：访问oceanbase的用户名
  - 必选：是
  - 默认值：无
- ** password**
  - 描述：访问oceanbase的密码
  - 必选：是
  - 默认值：无
- **readByPartition**
  - 描述：对分区表是否按照分区切分任务
  - 必选：否
  - 默认值：fasle
- **readBatchSize**
  - 描述：一次读取的行数，如果遇到内存不足的情况，可将该值调小
  - 必选：否
  - 默认值：10000
### 3.3 类
### 3.3 类型转换
下面列出OceanbaseReader针对Oceanbase类型转换列表:

| DataX 内部类型 | Oceanbase 数据类型 |
| --- | --- |
| Long | int |
| Double | numeric |
| String | varchar |
| Date | timestamp |
| Boolean | bool |

## 4性能测试
### 4.1 测试报告
影响速度的主要原因在于channel数量，channel值受限于分表的数量或者单个表的数据分片数量<br />单表导出时查看分片数量的办法,idb执行`select/*+query_timeout(150000000)*/ s.tablet_count from __all_table t,__table_stat s where t.table_id = s.table_id and t.table_name = '表名'`

| 通道数 | DataX速度(Rec/s) | DataX流量(MB/s) |
| --- | --- | --- |
| 1 | 15001 | 4.7 |
| 2 | 28169 | 11.66 |
| 3 | 37076 | 14.77 |
| 4 | 55862 | 17.60 |
| 5 | 70860 | 22.31 |

# 
