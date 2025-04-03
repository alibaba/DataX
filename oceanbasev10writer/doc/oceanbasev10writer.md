## 1 快速介绍
OceanBaseV10Writer 插件实现了写入数据到 OceanBase V1.0以及更高版本数据库的目的表的功能。在底层实现上， OceanbaseV10Writer 通过 java客户端(底层MySQL JDBC或oceanbase client) 连接obproxy远程 OceanBase 数据库，并执行相应的 insert .. on duplicate key update这条sql 语句将数据写入 OceanBase ，内部会分批次提交入库。
Oceanbasev10Writer 面向ETL开发工程师，他们使用 Oceanbasev10Writer 从数仓导入数据到 Oceanbase。同时 Oceanbasev10Writer 亦可以作为数据迁移工具为DBA等用户提供服务。

注意，oceanbasewriter是ob 0.5的writer，oceanbasev10writer是ob 1.0及以后版本的writer。

## 2 实现原理
Oceanbasev10Writer 通过 DataX 框架获取 Reader 生成的协议数据，生成insert ... on duplicate key update语句，在主键或唯一键冲突时，更新表中的所有字段。目前只有这一种行为，写入模式（只写入不更新）和更新指定字段目前暂未支持。 出于性能考虑，写入采用batch方式批量写，当行数累计到预定阈值时，才发起写入请求。
插件连接ob使用Mysql/Oceanbase JDBC driver通过obproxy连接ob；

## 3 功能说明
### 3.1 配置样例

- 这里使用一份从内存产生到 Oceanbase 导入的数据。
```
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            },
        "errorLimit": {
                "record": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "streamreader",
                    "parameter": {
                        "column" : [
                            {
                                "value": "DataX",
                                "type": "string"
                            },
                            {
                                "value": 19880808,
                                "type": "long"
                            },
                            {
                                "value": "1988-08-08 08:08:08",
                                "type": "date"
                            },
                            {
                                "value": true,
                                "type": "bool"
                            },
                            {
                                "value": "test",
                                "type": "bytes"
                            }
                        ],
                        "sliceRecordCount": 1000
                    }
                },
                "writer": {
                    "name": "oceanbasev10writer",
                    "parameter": {
                        "obWriteMode": "update",
                        "column": [
                            "id",
                            "name"
                        ],
                        "preSql": [
                            "delete from test"
                        ],
                        "connection": [
                            {
                                "jdbcUrl": "||_dsc_ob10_dsc_||集群名:租户名||_dsc_ob10_dsc_||jdbc:mysql://obproxyIp:obproxyPort/dbName",
                                "table": [
                                    "test"
                                ]
                            }
                        ],
                        "username": "xxx",
                        "password":"xxx",
                        "batchSize": 256,
                        "memstoreThreshold": "0.9"
                    }
                }
            }
        ]
    }
}
```
- 这里使用一份从内存产生到 Oceanbase 旁路导入的数据。
```
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            },
        "errorLimit": {
                "record": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "streamreader",
                    "parameter": {
                        "column" : [
                            {
                                "value": "DataX",
                                "type": "string"
                            },
                            {
                                "value": 19880808,
                                "type": "long"
                            },
                            {
                                "value": "1988-08-08 08:08:08",
                                "type": "date"
                            },
                            {
                                "value": true,
                                "type": "bool"
                            },
                            {
                                "value": "test",
                                "type": "bytes"
                            }
                        ],
                        "sliceRecordCount": 1000
                    }
                },
                "writer": {
                    "name": "oceanbasev10writer",
                    "parameter": {
                        "obWriteMode": "update",
                        "column": [
                            "id",
                            "name"
                        ],
                        "preSql": [
                            "delete from test"
                        ],
                        "connection": [
                            {
                                "jdbcUrl": "||_dsc_ob10_dsc_||集群名:租户名||_dsc_ob10_dsc_||jdbc:mysql://obproxyIp:obproxyPort/dbName",
                                "table": [
                                    "test"
                                ]
                            }
                        ],
                        "username": "xxx",
                        "password":"xxx",
                        "batchSize": 256,
                        "directPath": true,
                        "rpcPort": 2882,
                        "parallel": 8,
                        "heartBeatInterval": 1000,
                        "heartBeatTimeout": 6000,
                        "bufferSize": 1048576,
                        "memstoreThreshold": "0.9"
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
  - 描述：目的表的表名称。开源版obwriter插件仅支持写入一个表。表名中一般不含库名；
  - 必选：是
  - 默认值：无
- **column**
  - 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。
```
**column配置项必须指定，不能留空！**
   注意：1、我们强烈不推荐你这样配置，因为当你目的表字段个数、类型等有改动时，你的任务可能运行不正确或者失败
        2、 column 不能配置任何常量值
```

- 必选：是
- 默认值：否
- **preSql**
  - 描述：写入数据到目的表前，会先执行这里的标准语句。如果 Sql 中有你需要操作到的表名称，请使用 `@table` 表示，这样在实际执行 Sql 语句时，会对变量按照实际表名称进行替换。比如你的任务是要写入到目的端的100个同构分表(表名称为:datax_00,datax01, ... datax_98,datax_99)，并且你希望导入数据前，先对表中数据进行删除操作，那么你可以这样配置：`"preSql":["delete from @table"]`，效果是：在执行到每个表写入数据前，会先执行对应的 delete from 对应表名称.只支持delete语句
  - 必选：否
  - 默认值：无
- **batchSize**
  - 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与Oceanbase的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。
  - 必选：否
  - 默认值：1000
- **memstoreThreshold**
  - 描述：OB租户的memstore使用率，当达到这个阀值的时候暂停导入,等释放内存后继续导入. 防止租户内存溢出
  - 必选：否
  - 默认值：0.9
- **username**
  - 描述：访问oceanbase的用户名。注意当jdbcUrl配置为||_dsc_ob10_dsc_||集群名:租户名||_dsc_ob10_dsc_||这样的格式时，此处不配置ob的集群名和租户名。否则需要配置为三段式形式。
  - 必选：是
  - 默认值：无
- **** password****
  - 描述：访问oceanbase的密码
  - 必选：是
  - 默认值：无
- writerThreadCount
  - 描述：每个通道（channel）中写入使用的线程数
  - 必选：否
  - 默认值：1
- directPath
  - 描述：开启旁路导入
  - 必选：否
  - 默认值：false
- rpcPort
  - 描述：oceanbase的rpc端口
  - 必选：否
  - 默认值：无
- parallel
  - 描述：旁路导入的启用线程数
  - 必选：否
  - 默认值：1
- bufferSize
  - 描述：旁路导入的切分数据块大小
  - 必选：否
  - 默认值：1048576
- heartBeatInterval
  - 描述：旁路导入的心跳间隔
  - 必选：否
  - 默认值：1000
- heartBeatTimeout
  - 描述：旁路导入的心跳超时时间
  - 必选：否
  - 默认值：6000
```
**开启了旁路导入，即directPath:true时**
   注意：1、此时rpcPort为必填项。
        2、设置parallel时，parallel和oceanbase的负载有关。
        3、设置heartBeatTimeout最低不能低于6000，heartBeatTimeout的值最低不能低于1000，
        当heartBeatTimeout和heartBeatTimeout同时设置时，heartBeatTimeout-heartBeatTimeout的差值不能低于4000。
        4、bufferSize的单位为字节数，默认为1M，即1048576。
```

## 4 常见问题
### 
4.1 连接断开导致写入失败
Data X写入ob的任务失败，在log中可以发现在写入ob时，连接被断开：
```
2018-12-14 05:40:48.586 [18705170-3-17-writer] WARN  CommonRdbmsWriter$Task - 遇到OB异常,回滚此次写入, 休眠 1秒,采用逐条写入提交,SQLState:S1000
java.sql.SQLException: Could not retrieve transation read-only status server
	at com.mysql.jdbc.SQLError.createSQLException(SQLError.java:964) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	at com.mysql.jdbc.SQLError.createSQLException(SQLError.java:897) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	at com.mysql.jdbc.SQLError.createSQLException(SQLError.java:886) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	at com.mysql.jdbc.SQLError.createSQLException(SQLError.java:860) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	at com.mysql.jdbc.SQLError.createSQLException(SQLError.java:877) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	at com.mysql.jdbc.SQLError.createSQLException(SQLError.java:873) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	at com.mysql.jdbc.ConnectionImpl.isReadOnly(ConnectionImpl.java:3603) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	at com.mysql.jdbc.ConnectionImpl.isReadOnly(ConnectionImpl.java:3572) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	at com.mysql.jdbc.PreparedStatement.executeBatchInternal(PreparedStatement.java:1225) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	at com.mysql.jdbc.StatementImpl.executeBatch(StatementImpl.java:958) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	at com.alibaba.datax.plugin.writer.oceanbasev10writer.task.MultiTableWriterTask.write(MultiTableWriterTask.java:357) [oceanbasev10writer-0.0.1-SNAPSHOT.jar:na]
	at com.alibaba.datax.plugin.writer.oceanbasev10writer.task.MultiTableWriterTask.calcRuleAndDoBatchInsert(MultiTableWriterTask.java:338) [oceanbasev10writer-0.0.1-SNAPSHOT.jar:na]
	at com.alibaba.datax.plugin.writer.oceanbasev10writer.task.MultiTableWriterTask.startWrite(MultiTableWriterTask.java:227) [oceanbasev10writer-0.0.1-SNAPSHOT.jar:na]
	at com.alibaba.datax.plugin.writer.oceanbasev10writer.OceanBaseV10Writer$Task.startWrite(OceanBaseV10Writer.java:360) [oceanbasev10writer-0.0.1-SNAPSHOT.jar:na]
	at com.alibaba.datax.core.taskgroup.runner.WriterRunner.run(WriterRunner.java:62) [datax-core-0.0.1-SNAPSHOT.jar:na]
	at java.lang.Thread.run(Thread.java:834) [na:1.8.0_112]
Caused by: com.mysql.jdbc.exceptions.jdbc4.CommunicationsException: Communications link failure
The last packet successfully received from the server was 5 milliseconds ago.  The last packet sent successfully to the server was 4 milliseconds ago.
	at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method) ~[na:1.8.0_112]
	at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62) ~[na:1.8.0_112]
	at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45) ~[na:1.8.0_112]
	at java.lang.reflect.Constructor.newInstance(Constructor.java:423) ~[na:1.8.0_112]
	at com.mysql.jdbc.Util.handleNewInstance(Util.java:425) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	at com.mysql.jdbc.SQLError.createCommunicationsException(SQLError.java:989) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	at com.mysql.jdbc.MysqlIO.reuseAndReadPacket(MysqlIO.java:3556) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	at com.mysql.jdbc.MysqlIO.reuseAndReadPacket(MysqlIO.java:3456) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	at com.mysql.jdbc.MysqlIO.checkErrorPacket(MysqlIO.java:3897) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	at com.mysql.jdbc.MysqlIO.sendCommand(MysqlIO.java:2524) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	at com.mysql.jdbc.MysqlIO.sqlQueryDirect(MysqlIO.java:2677) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	at com.mysql.jdbc.ConnectionImpl.execSQL(ConnectionImpl.java:2545) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	at com.mysql.jdbc.ConnectionImpl.execSQL(ConnectionImpl.java:2503) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	at com.mysql.jdbc.StatementImpl.executeQuery(StatementImpl.java:1369) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	at com.mysql.jdbc.ConnectionImpl.isReadOnly(ConnectionImpl.java:3597) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	... 9 common frames omitted
Caused by: java.io.EOFException: Can not read response from server. Expected to read 4 bytes, read 0 bytes before connection was unexpectedly lost.
	at com.mysql.jdbc.MysqlIO.readFully(MysqlIO.java:3008) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	at com.mysql.jdbc.MysqlIO.reuseAndReadPacket(MysqlIO.java:3466) ~[mysql-connector-java-5.1.40.jar:5.1.40]
	... 17 common frames omitted
```
关键字：could not retrieve transation status from read-only status server, communication link failure
检查运行Data X任务的机器，发现obproxy在任务运行时发生若干次重启：
![](https://cdn.nlark.com/lark/0/2018/png/97504/1544760936504-948a2699-e21b-4970-ad76-25b6ac1cd89d.png#height=156&id=wutJw&originHeight=156&originWidth=507&originalType=binary&ratio=1&rotation=0&showTitle=false&status=done&style=none&title=&width=507)
在第一次obproxy退出的日志里，找到退出原因：
```
[2018-12-14 05:40:47.611683] ERROR [PROXY] do_monitor_mem (ob_proxy_main.cpp:889) [7262][Y0-7F4480213880] [AL=47391-47390-29] obproxy's memroy is out of limit, will be going to commit suicide(mem_limited=838860800, OTHER_MEMORY_SIZE=73400320, is_out_of_mem_limit=true, cur_pos=9) BACKTRACE:0x49db91 0x47fdc9 0x43b115 0x43ee5d 0xa6e623 0xe401b2 0xe3f497 0x4f674c 0x7f4487ace77d 0x7f44865ed9ad
[2018-12-14 05:40:47.612334] ERROR [PROXY] do_monitor_mem (ob_proxy_main.cpp:891) [7262][Y0-7F4480213880] [AL=47392-47391-651] history memory size, history_mem_size[0]=765460480 BACKTRACE:0x49db91 0x47fdc9 0x48717a 0x43f121 0xa6e623 0xe401b2 0xe3f497 0x4f674c 0x7f4487ace77d 0x7f44865ed9ad
[2018-12-14 05:40:47.612934] ERROR [PROXY] do_monitor_mem (ob_proxy_main.cpp:891) [7262][Y0-7F4480213880] [AL=47393-47392-600] history memory size, history_mem_size[1]=765460480 BACKTRACE:0x49db91 0x47fdc9 0x48717a 0x43f121 0xa6e623 0xe401b2 0xe3f497 0x4f674c 0x7f4487ace77d 0x7f44865ed9ad
[2018-12-14 05:40:47.613530] ERROR [PROXY] do_monitor_mem (ob_proxy_main.cpp:891) [7262][Y0-7F4480213880] [AL=47394-47393-596] history memory size, history_mem_size[2]=765460480 BACKTRACE:0x49db91 0x47fdc9 0x48717a 0x43f121 0xa6e623 0xe401b2 0xe3f497 0x4f674c 0x7f4487ace77d 0x7f44865ed9ad
[2018-12-14 05:40:47.614121] ERROR [PROXY] do_monitor_mem (ob_proxy_main.cpp:891) [7262][Y0-7F4480213880] [AL=47395-47394-591] history memory size, history_mem_size[3]=765460480 BACKTRACE:0x49db91 0x47fdc9 0x48717a 0x43f121 0xa6e623 0xe401b2 0xe3f497 0x4f674c 0x7f4487ace77d 0x7f44865ed9ad
[2018-12-14 05:40:47.614717] ERROR [PROXY] do_monitor_mem (ob_proxy_main.cpp:891) [7262][Y0-7F4480213880] [AL=47396-47395-596] history memory size, history_mem_size[4]=765460480 BACKTRACE:0x49db91 0x47fdc9 0x48717a 0x43f121 0xa6e623 0xe401b2 0xe3f497 0x4f674c 0x7f4487ace77d 0x7f44865ed9ad
[2018-12-14 05:40:47.615307] ERROR [PROXY] do_monitor_mem (ob_proxy_main.cpp:891) [7262][Y0-7F4480213880] [AL=47397-47396-590] history memory size, history_mem_size[5]=765460480 BACKTRACE:0x49db91 0x47fdc9 0x48717a 0x43f121 0xa6e623 0xe401b2 0xe3f497 0x4f674c 0x7f4487ace77d 0x7f44865ed9ad
```
关键字：obproxy's memroy is out of limit, will be going to commit suicide
可以看到，obproxy由于内存不足退出。
#### 解决方案
obproxy在启动时， 可以指定使用内存上限，默认是800M，在某些情况下，比如连接数较多（该失败的任务为写入100张分表，并发数32，因此连接数为3200），可能会导致obproxy内存不够用。要解决该问题，一方面可以调低任务的并发数，另一方面可以调大obproxy的内存限制，比如调整至2G。

### 4.2 Session interrupted
在使用ob 1.0 writer往单表里写入数据时，遇到以下错误：

```
2019-01-03 19:37:27.197 [0-insertTask-73] WARN  InsertTask - Insert fatal error SqlState =HY000, errorCode = 5066, java.sql.SQLException: Session interrupted, server ip:port[11.145.28.93:2881]
```
关键字：fatal，Session interrupted，server ip:port
在任务执行的log中，还可以发现如下log：

```
2019-08-09 11:56:56.758 [2-insertTask-82] ERROR StdoutPluginCollector - 
java.sql.SQLException: Session interrupted, server ip:port[11.232.58.16:2881]
	at com.alipay.oceanbase.obproxy.connection.ObGroupConnection.checkAndThrowException(ObGroupConnection.java:431) ~[oceanbase-connector-java-2.0.8.20180730.jar:na]
	at com.alipay.oceanbase.obproxy.statement.ObStatement.doExecute(ObStatement.java:598) ~[oceanbase-connector-java-2.0.8.20180730.jar:na]
	at com.alipay.oceanbase.obproxy.statement.ObStatement.execute(ObStatement.java:456) ~[oceanbase-connector-java-2.0.8.20180730.jar:na]
	at com.alipay.oceanbase.obproxy.statement.ObPreparedStatement.execute(ObPreparedStatement.java:148) ~[oceanbase-connector-java-2.0.8.20180730.jar:na]
	at com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter$Task.doOneInsert(CommonRdbmsWriter.java:430) ~[plugin-rdbms-util-0.0.1-SNAPSHOT.jar:na]
	at com.alibaba.datax.plugin.writer.oceanbasev10writer.task.InsertTask.doMultiInsert(InsertTask.java:196) [oceanbasev10writer-0.0.1-SNAPSHOT.jar:na]
	at com.alibaba.datax.plugin.writer.oceanbasev10writer.task.InsertTask.run(InsertTask.java:85) [oceanbasev10writer-0.0.1-SNAPSHOT.jar:na]
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1147) [na:1.8.0_112]
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:622) [na:1.8.0_112]
	at java.lang.Thread.run(Thread.java:834) [na:1.8.0_112]
Caused by: com.alipay.oceanbase.obproxy.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException: INSERT command denied to user 'dwexp'@'%' for table 'mobile_product_version_info'
	at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method) ~[na:1.8.0_112]
	at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62) ~[na:1.8.0_112]
	at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45) ~[na:1.8.0_112]
	at java.lang.reflect.Constructor.newInstance(Constructor.java:423) ~[na:1.8.0_112]
	at com.alipay.oceanbase.obproxy.mysql.jdbc.Util.handleNewInstance(Util.java:409) ~[oceanbase-connector-java-2.0.8.20180730.jar:na]
	at com.alipay.oceanbase.obproxy.mysql.jdbc.Util.getInstance(Util.java:384) ~[oceanbase-connector-java-2.0.8.20180730.jar:na]
	at com.alipay.oceanbase.obproxy.mysql.jdbc.SQLError.createSQLException(SQLError.java:1052) ~[oceanbase-connector-java-2.0.8.20180730.jar:na]
	at com.alipay.oceanbase.obproxy.mysql.jdbc.MysqlIO.checkErrorPacket(MysqlIO.java:4403) ~[oceanbase-connector-java-2.0.8.20180730.jar:na]
	at com.alipay.oceanbase.obproxy.mysql.jdbc.MysqlIO.checkErrorPacket(MysqlIO.java:4275) ~[oceanbase-connector-java-2.0.8.20180730.jar:na]
	at com.alipay.oceanbase.obproxy.mysql.jdbc.MysqlIO.sendCommand(MysqlIO.java:2706) ~[oceanbase-connector-java-2.0.8.20180730.jar:na]
	at com.alipay.oceanbase.obproxy.mysql.jdbc.MysqlIO.sqlQueryDirect(MysqlIO.java:2867) ~[oceanbase-connector-java-2.0.8.20180730.jar:na]
	at com.alipay.oceanbase.obproxy.mysql.jdbc.ConnectionImpl.execSQL(ConnectionImpl.java:2843) ~[oceanbase-connector-java-2.0.8.20180730.jar:na]
	at com.alipay.oceanbase.obproxy.mysql.jdbc.PreparedStatement.executeInternal(PreparedStatement.java:2085) ~[oceanbase-connector-java-2.0.8.20180730.jar:na]
	at com.alipay.oceanbase.obproxy.mysql.jdbc.PreparedStatement.execute(PreparedStatement.java:1310) ~[oceanbase-connector-java-2.0.8.20180730.jar:na]
	at com.alipay.oceanbase.obproxy.druid.pool.DruidPooledPreparedStatement.execute(DruidPooledPreparedStatement.java:493) ~[oceanbase-connector-java-2.0.8.20180730.jar:na]
	at com.alipay.oceanbase.obproxy.statement.ObPreparedStatement.executeOnConnection(ObPreparedStatement.java:121) ~[oceanbase-connector-java-2.0.8.20180730.jar:na]
	at com.alipay.oceanbase.obproxy.statement.ObStatement.doExecuteOnConnection(ObStatement.java:677) ~[oceanbase-connector-java-2.0.8.20180730.jar:na]
	at com.alipay.oceanbase.obproxy.statement.ObStatement.doExecute(ObStatement.java:558) ~[oceanbase-connector-java-2.0.8.20180730.jar:na]
	... 8 common frames omitted
```
可以看到，异常是由于没有insert权限（INSERT command denied to user 'dwexp'@'%' for table）引起的。

关键字：INSERT command denied to user 'dwexp'@'%'
可以看到这个错误是由于没有写入权限导致的，因此在observer的log、obproxy的log中都没有相关的信息。
#### 解决方案
在ob中给相关用户授权之后，任务重试即可成功。

参考授权命令为：
```sql
grant select, insert, update on dbName.tableName to dwexp;
grant select on oceanbase.gv$memstore to dwexp;
```
