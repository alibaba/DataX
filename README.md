![Datax-logo](https://github.com/alibaba/DataX/blob/master/images/DataX-logo.jpg)

# DataX

[![Leaderboard](https://img.shields.io/badge/DataX-%E6%9F%A5%E7%9C%8B%E8%B4%A1%E7%8C%AE%E6%8E%92%E8%A1%8C%E6%A6%9C-orange)](https://opensource.alibaba.com/contribution_leaderboard/details?projectValue=datax)

DataX 是阿里云 [DataWorks数据集成](https://www.aliyun.com/product/bigdata/ide) 的开源版本，在阿里巴巴集团内被广泛使用的离线数据同步工具/平台。DataX 实现了包括 MySQL、Oracle、OceanBase、SqlServer、Postgre、HDFS、Hive、ADS、HBase、TableStore(OTS)、MaxCompute(ODPS)、Hologres、DRDS, databend 等各种异构数据源之间高效的数据同步功能。

# DataX 商业版本
阿里云DataWorks数据集成是DataX团队在阿里云上的商业化产品，致力于提供复杂网络环境下、丰富的异构数据源之间高速稳定的数据移动能力，以及繁杂业务背景下的数据同步解决方案。目前已经支持云上近3000家客户，单日同步数据超过3万亿条。DataWorks数据集成目前支持离线50+种数据源，可以进行整库迁移、批量上云、增量同步、分库分表等各类同步解决方案。2020年更新实时同步能力，支持10+种数据源的读写任意组合。提供MySQL，Oracle等多种数据源到阿里云MaxCompute，Hologres等大数据引擎的一键全增量同步解决方案。

商业版本参见：  https://www.aliyun.com/product/bigdata/ide


# Features

DataX本身作为数据同步框架，将不同数据源的同步抽象为从源头数据源读取数据的Reader插件，以及向目标端写入数据的Writer插件，理论上DataX框架可以支持任意数据源类型的数据同步工作。同时DataX插件体系作为一套生态系统, 每接入一套新数据源该新加入的数据源即可实现和现有的数据源互通。



# DataX详细介绍

##### 请参考：[DataX-Introduction](https://github.com/alibaba/DataX/blob/master/introduction.md)



# Quick Start

##### Download [DataX下载地址](https://datax-opensource.oss-cn-hangzhou.aliyuncs.com/202308/datax.tar.gz)


##### 请点击：[Quick Start](https://github.com/alibaba/DataX/blob/master/userGuid.md)



# Support Data Channels 

DataX目前已经有了比较全面的插件体系，主流的RDBMS数据库、NOSQL、大数据计算系统都已经接入，目前支持数据如下图，详情请点击：[DataX数据源参考指南](https://github.com/alibaba/DataX/wiki/DataX-all-data-channels)

| 类型               | 数据源                          | Reader(读) | Writer(写) |                                                                                                                       文档                                                                                                                       |
|--------------|---------------------------|:---------:|:---------:|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
| RDBMS 关系型数据库 | MySQL                           |     √      |     √      |                                       [读](https://github.com/alibaba/DataX/blob/master/mysqlreader/doc/mysqlreader.md) 、[写](https://github.com/alibaba/DataX/blob/master/mysqlwriter/doc/mysqlwriter.md)                                       |
|                    | Oracle                          |     √      |     √      |                                     [读](https://github.com/alibaba/DataX/blob/master/oraclereader/doc/oraclereader.md) 、[写](https://github.com/alibaba/DataX/blob/master/oraclewriter/doc/oraclewriter.md)                                     |
|                    | OceanBase                       |     √      |     √      | [读](https://open.oceanbase.com/docs/community/oceanbase-database/V3.1.0/use-datax-to-full-migration-data-to-oceanbase) 、[写](https://open.oceanbase.com/docs/community/oceanbase-database/V3.1.0/use-datax-to-full-migration-data-to-oceanbase) |
|                    | SQLServer                       |     √      |     √      |                               [读](https://github.com/alibaba/DataX/blob/master/sqlserverreader/doc/sqlserverreader.md) 、[写](https://github.com/alibaba/DataX/blob/master/sqlserverwriter/doc/sqlserverwriter.md)                               |
|                    | PostgreSQL                      |     √      |     √      |                             [读](https://github.com/alibaba/DataX/blob/master/postgresqlreader/doc/postgresqlreader.md) 、[写](https://github.com/alibaba/DataX/blob/master/postgresqlwriter/doc/postgresqlwriter.md)                             |
|                    | DRDS                            |     √      |     √      |                                         [读](https://github.com/alibaba/DataX/blob/master/drdsreader/doc/drdsreader.md) 、[写](https://github.com/alibaba/DataX/blob/master/drdswriter/doc/drdswriter.md)                                         |
|                    | Kingbase                        |     √      |     √      |                                         [读](https://github.com/alibaba/DataX/blob/master/drdsreader/doc/drdsreader.md) 、[写](https://github.com/alibaba/DataX/blob/master/drdswriter/doc/drdswriter.md)                                         |
|                    | 通用RDBMS(支持所有关系型数据库) |     √      |     √      |                                       [读](https://github.com/alibaba/DataX/blob/master/rdbmsreader/doc/rdbmsreader.md) 、[写](https://github.com/alibaba/DataX/blob/master/rdbmswriter/doc/rdbmswriter.md)                                       |
| 阿里云数仓数据存储 | ODPS                            |     √      |     √      |                                         [读](https://github.com/alibaba/DataX/blob/master/odpsreader/doc/odpsreader.md) 、[写](https://github.com/alibaba/DataX/blob/master/odpswriter/doc/odpswriter.md)                                         |
|                    | ADB                             |            |     √      |                                                                             [写](https://github.com/alibaba/DataX/blob/master/adbmysqlwriter/doc/adbmysqlwriter.md)                                                                             |
|                    | ADS                             |            |     √      |                                                                                  [写](https://github.com/alibaba/DataX/blob/master/adswriter/doc/adswriter.md)                                                                                  |
|                    | OSS                             |     √      |     √      |                                           [读](https://github.com/alibaba/DataX/blob/master/ossreader/doc/ossreader.md) 、[写](https://github.com/alibaba/DataX/blob/master/osswriter/doc/osswriter.md)                                           |
|                    | OCS                             |            |     √      |                                                                                  [写](https://github.com/alibaba/DataX/blob/master/ocswriter/doc/ocswriter.md)                                                                                  |
|                    | Hologres                        |            |     √      |                                                                         [写](https://github.com/alibaba/DataX/blob/master/hologresjdbcwriter/doc/hologresjdbcwriter.md)                                                                         |
|                    | AnalyticDB For PostgreSQL       |            |     √      |                                                                                                                       写                                                                                                                        |
| 阿里云中间件       | datahub                         |     √      |     √      |                                                                                                                      读 、写                                                                                                                      |
|                    | SLS                             |     √      |     √      |                                                                                                                      读 、写                                                                                                                      |
| 图数据库           | 阿里云 GDB                      |     √      |     √      |                                           [读](https://github.com/alibaba/DataX/blob/master/gdbreader/doc/gdbreader.md) 、[写](https://github.com/alibaba/DataX/blob/master/gdbwriter/doc/gdbwriter.md)                                           |
|                    | Neo4j                           |            |     √      |                                                                                [写](https://github.com/alibaba/DataX/blob/master/neo4jwriter/doc/neo4jwriter.md)                                                                                |
| NoSQL数据存储      | OTS                             |     √      |     √      |                                           [读](https://github.com/alibaba/DataX/blob/master/otsreader/doc/otsreader.md) 、[写](https://github.com/alibaba/DataX/blob/master/otswriter/doc/otswriter.md)                                           |
|                    | Hbase0.94                       |     √      |     √      |                               [读](https://github.com/alibaba/DataX/blob/master/hbase094xreader/doc/hbase094xreader.md) 、[写](https://github.com/alibaba/DataX/blob/master/hbase094xwriter/doc/hbase094xwriter.md)                               |
|                    | Hbase1.1                        |     √      |     √      |                                 [读](https://github.com/alibaba/DataX/blob/master/hbase11xreader/doc/hbase11xreader.md) 、[写](https://github.com/alibaba/DataX/blob/master/hbase11xwriter/doc/hbase11xwriter.md)                                 |
|                    | Phoenix4.x                      |     √      |     √      |                           [读](https://github.com/alibaba/DataX/blob/master/hbase11xsqlreader/doc/hbase11xsqlreader.md) 、[写](https://github.com/alibaba/DataX/blob/master/hbase11xsqlwriter/doc/hbase11xsqlwriter.md)                           |
|                    | Phoenix5.x                      |     √      |     √      |                           [读](https://github.com/alibaba/DataX/blob/master/hbase20xsqlreader/doc/hbase20xsqlreader.md) 、[写](https://github.com/alibaba/DataX/blob/master/hbase20xsqlwriter/doc/hbase20xsqlwriter.md)                           |
|                    | MongoDB                         |     √      |     √      |                                   [读](https://github.com/alibaba/DataX/blob/master/mongodbreader/doc/mongodbreader.md) 、[写](https://github.com/alibaba/DataX/blob/master/mongodbwriter/doc/mongodbwriter.md)                                   |
|                    | Cassandra                       |     √      |     √      |                               [读](https://github.com/alibaba/DataX/blob/master/cassandrareader/doc/cassandrareader.md) 、[写](https://github.com/alibaba/DataX/blob/master/cassandrawriter/doc/cassandrawriter.md)                               |
| 数仓数据存储       | StarRocks                       |     √      |     √      |                                                                          读 、[写](https://github.com/alibaba/DataX/blob/master/starrockswriter/doc/starrockswriter.md)                                                                           |
|                    | ApacheDoris                     |            |     √      |                                                                                [写](https://github.com/alibaba/DataX/blob/master/doriswriter/doc/doriswriter.md)                                                                                |
|                    | ClickHouse                      |     √      |     √      |                              [读](https://github.com/alibaba/DataX/blob/master/clickhousereader/doc/clickhousereader.md) 、[写](https://github.com/alibaba/DataX/blob/master/clickhousewriter/doc/clickhousewriter.md)                               |
|                    | Databend                        |            |     √      |                                                                             [写](https://github.com/alibaba/DataX/blob/master/databendwriter/doc/databendwriter.md)                                                                             |
|                    | Hive                            |     √      |     √      |                                         [读](https://github.com/alibaba/DataX/blob/master/hdfsreader/doc/hdfsreader.md) 、[写](https://github.com/alibaba/DataX/blob/master/hdfswriter/doc/hdfswriter.md)                                         |
|                    | kudu                            |            |     √      |                                                                                 [写](https://github.com/alibaba/DataX/blob/master/hdfswriter/doc/hdfswriter.md)                                                                                 |
|                    | selectdb                        |            |     √      |                                                                             [写](https://github.com/alibaba/DataX/blob/master/selectdbwriter/doc/selectdbwriter.md)                                                                             |
| 无结构化数据存储   | TxtFile                         |     √      |     √      |                                   [读](https://github.com/alibaba/DataX/blob/master/txtfilereader/doc/txtfilereader.md) 、[写](https://github.com/alibaba/DataX/blob/master/txtfilewriter/doc/txtfilewriter.md)                                   |
|                    | FTP                             |     √      |     √      |                                           [读](https://github.com/alibaba/DataX/blob/master/ftpreader/doc/ftpreader.md) 、[写](https://github.com/alibaba/DataX/blob/master/ftpwriter/doc/ftpwriter.md)                                           |
|                    | HDFS                            |     √      |     √      |                                         [读](https://github.com/alibaba/DataX/blob/master/hdfsreader/doc/hdfsreader.md) 、[写](https://github.com/alibaba/DataX/blob/master/hdfswriter/doc/hdfswriter.md)                                         |
|                    | Elasticsearch                   |            |     √      |                                                                        [写](https://github.com/alibaba/DataX/blob/master/elasticsearchwriter/doc/elasticsearchwriter.md)                                                                        |
| 时间序列数据库     | OpenTSDB                        |     √      |            |                                                                             [读](https://github.com/alibaba/DataX/blob/master/opentsdbreader/doc/opentsdbreader.md)                                                                             |
|                    | TSDB                            |     √      |     √      |                                       [读](https://github.com/alibaba/DataX/blob/master/tsdbreader/doc/tsdbreader.md) 、[写](https://github.com/alibaba/DataX/blob/master/tsdbwriter/doc/tsdbhttpwriter.md)                                       |
|                    | TDengine                        |     √      |     √      |                              [读](https://github.com/alibaba/DataX/blob/master/tdenginereader/doc/tdenginereader-CN.md) 、[写](https://github.com/alibaba/DataX/blob/master/tdenginewriter/doc/tdenginewriter-CN.md)                              |

# 阿里云DataWorks数据集成

目前DataX的已有能力已经全部融和进阿里云的数据集成，并且比DataX更加高效、安全，同时数据集成具备DataX不具备的其它高级特性和功能。可以理解为数据集成是DataX的全面升级的商业化用版本，为企业可以提供稳定、可靠、安全的数据传输服务。与DataX相比，数据集成主要有以下几大突出特点：

支持实时同步：

- 功能简介：https://help.aliyun.com/document_detail/181912.html
- 支持的数据源：https://help.aliyun.com/document_detail/146778.html
- 支持数据处理：https://help.aliyun.com/document_detail/146777.html

离线同步数据源种类大幅度扩充：

- 新增比如：DB2、Kafka、Hologres、MetaQ、SAPHANA、达梦等等，持续扩充中
- 离线同步支持的数据源：https://help.aliyun.com/document_detail/137670.html
- 具备同步解决方案：
    - 解决方案系统：https://help.aliyun.com/document_detail/171765.html
    - 一键全增量：https://help.aliyun.com/document_detail/175676.html
    - 整库迁移：https://help.aliyun.com/document_detail/137809.html
    - 批量上云：https://help.aliyun.com/document_detail/146671.html
    - 更新更多能力请访问：https://help.aliyun.com/document_detail/137663.html
    -

# 我要开发新的插件

请点击：[DataX插件开发宝典](https://github.com/alibaba/DataX/blob/master/dataxPluginDev.md)

# 重要版本更新说明

DataX 后续计划月度迭代更新，也欢迎感兴趣的同学提交 Pull requests，月度更新内容如下。

- [datax_v202309]（https://github.com/alibaba/DataX/releases/tag/datax_v202309)
  - 支持Phoenix 同步数据添加 where条件
  - 支持华为 GuassDB读写插件
  - 修复ClickReader 插件运行报错 Can't find bundle for base name
  - 增加 DataX调试模块
  - 修复 orc空文件报错问题
  - 优化obwriter性能
  - txtfilewriter 增加导出为insert语句功能支持
  - HdfsReader/HdfsWriter 支持parquet读写能力
  
- [datax_v202308]（https://github.com/alibaba/DataX/releases/tag/datax_v202308)
  - OTS 插件更新
  - databend 插件更新
  - Oceanbase驱动修复


- [datax_v202306]（https://github.com/alibaba/DataX/releases/tag/datax_v202306)
  - 精简代码
  - 新增插件（neo4jwriter、clickhousewriter）
  - 优化插件、修复问题（oceanbase、hdfs、databend、txtfile）


- [datax_v202303]（https://github.com/alibaba/DataX/releases/tag/datax_v202303)
  - 精简代码
  - 新增插件（adbmysqlwriter、databendwriter、selectdbwriter）
  - 优化插件、修复问题（sqlserver、hdfs、cassandra、kudu、oss）
  - fastjson 升级到 fastjson2

- [datax_v202210]（https://github.com/alibaba/DataX/releases/tag/datax_v202210)
  - 涉及通道能力更新（OceanBase、Tdengine、Doris等）

- [datax_v202209]（https://github.com/alibaba/DataX/releases/tag/datax_v202209)
    - 涉及通道能力更新（MaxCompute、Datahub、SLS等）、安全漏洞更新、通用打包更新等

- [datax_v202205]（https://github.com/alibaba/DataX/releases/tag/datax_v202205)
    - 涉及通道能力更新（MaxCompute、Hologres、OSS、Tdengine等）、安全漏洞更新、通用打包更新等


# 项目成员

核心Contributions: 言柏 、枕水、秋奇、青砾、一斅、云时

感谢天烬、光戈、祁然、巴真、静行对DataX做出的贡献。

# License

This software is free to use under the Apache License [Apache license](https://github.com/alibaba/DataX/blob/master/license.txt).

# 
请及时提出issue给我们。请前往：[DataxIssue](https://github.com/alibaba/DataX/issues)

# 开源版DataX企业用户

![Datax-logo](https://github.com/alibaba/DataX/blob/master/images/datax-enterprise-users.jpg)

```
长期招聘 联系邮箱：datax@alibabacloud.com
【JAVA开发职位】
职位名称：JAVA资深开发工程师/专家/高级专家
工作年限 : 2年以上
学历要求 : 本科（如果能力靠谱，这些都不是条件）
期望层级 : P6/P7/P8

岗位描述：
    1. 负责阿里云大数据平台（数加）的开发设计。 
    2. 负责面向政企客户的大数据相关产品开发；
    3. 利用大规模机器学习算法挖掘数据之间的联系，探索数据挖掘技术在实际场景中的产品应用 ；
    4. 一站式大数据开发平台
    5. 大数据任务调度引擎
    6. 任务执行引擎
    7. 任务监控告警
    8. 海量异构数据同步

岗位要求：
    1. 拥有3年以上JAVA Web开发经验；
    2. 熟悉Java的基础技术体系。包括JVM、类装载、线程、并发、IO资源管理、网络；
    3. 熟练使用常用Java技术框架、对新技术框架有敏锐感知能力；深刻理解面向对象、设计原则、封装抽象；
    4. 熟悉HTML/HTML5和JavaScript；熟悉SQL语言；
    5. 执行力强，具有优秀的团队合作精神、敬业精神；
    6. 深刻理解设计模式及应用场景者加分；
    7. 具有较强的问题分析和处理能力、比较强的动手能力，对技术有强烈追求者优先考虑；
    8. 对高并发、高稳定可用性、高性能、大数据处理有过实际项目及产品经验者优先考虑；
    9. 有大数据产品、云产品、中间件技术解决方案者优先考虑。
````

用户咨询支持：

钉钉群目前暂时受到了一些管控策略影响，建议大家有问题优先在这里提交问题 Issue，DataX研发和社区会定期回答Issue中的问题，知识库丰富后也能帮助到后来的使用者。



