## TableStore增量数据导出通道：TableStoreStreamReader

### 快速介绍

TableStoreStreamReader插件主要用于TableStore的增量数据导出，增量数据可以看作操作日志，除了数据本身外还附有操作信息。

与全量导出插件不同，增量导出插件只有多版本模式，同时不支持指定列。这是与增量导出的原理有关的，导出的格式下面有详细介绍。

使用插件前必须确保表上已经开启Stream功能，可以在建表的时候指定开启，或者使用SDK的UpdateTable接口开启。

    开启Stream的方法：
    SyncClient client = new SyncClient("", "", "", "");
    1. 建表的时候开启：
    CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta);
    createTableRequest.setStreamSpecification(new StreamSpecification(true, 24)); // 24代表增量数据保留24小时
    client.createTable(createTableRequest);
    
    2. 如果建表时未开启，可以通过UpdateTable开启:
    UpdateTableRequest updateTableRequest = new UpdateTableRequest("tableName");
    updateTableRequest.setStreamSpecification(new StreamSpecification(true, 24)); 
    client.updateTable(updateTableRequest);

### 实现原理

首先用户使用SDK的UpdateTable功能，指定开启Stream并设置过期时间，即开启了增量功能。

开启后，TableStore服务端就会将用户的操作日志额外保存起来，
每个分区有一个有序的操作日志队列，每条操作日志会在一定时间后被垃圾回收，这个时间即用户指定的过期时间。

TableStore的SDK提供了几个Stream相关的API用于将这部分操作日志读取出来，增量插件也是通过TableStore SDK的接口获取到增量数据的，并将
增量数据转化为多个6元组的形式(pk, colName, version, colValue, opType, sequenceInfo)导入到ODPS中。

### Reader的配置模版：

    "reader": {
        "name" : "otsstreamreader",
        "parameter" : {
            "endpoint" : "",
            "accessId" : "",
            "accessKey" : "",
            "instanceName" : "",
            //dataTable即需要导出数据的表。
            "dataTable" : "",  
            //statusTable是Reader用于保存状态的表，若该表不存在，Reader会自动创建该表。
            //一次离线导出任务完成后，用户不应删除该表，该表中记录的状态可用于下次导出任务中。
            "statusTable" : "TableStoreStreamReaderStatusTable",  
            //增量数据的时间范围（左闭右开）的左边界。
            "startTimestampMillis" : "",
            //增量数据的时间范围（左闭右开）的右边界。
            "endTimestampMillis" : "",
            //采云间调度只支持天级别，所以提供该配置，作用与startTimestampMillis和endTimestampMillis类似。
            "date": "",
            //是否导出时序信息。
            "isExportSequenceInfo": true,
            //从TableStore中读增量数据时，每次请求的最大重试次数，默认为30。
            "maxRetries" : 30
        }
    }

### 参数说明

| 名称 | 说明 | 类型 | 必选 |
| ---- | ---- | ---- | ---- |
| endpoint | TableStoreServer的Endpoint地址。| String | 是 |
| accessId | 用于访问TableStore服务的accessId。| String | 是 |
| accessKey | 用于访问TableStore服务的accessKey。 | String | 是 |
| instanceName | TableStore的实例名称。 | String | 是 |
| dataTable | 需要导出增量数据的表的名称。该表需要开启Stream，可以在建表时开启，或者使用UpdateTable接口开启。 | String | 是 |
| statusTable | Reader插件用于记录状态的表的名称，这些状态可用于减少对非目标范围内的数据的扫描，从而加快导出速度。<br> 1. 用户不需要创建该表，只需要给出一个表名。Reader插件会尝试在用户的instance下创建该表，若该表不存在即创建新表，若该表已存在，会判断该表的Meta是否与期望一致，若不一致会抛出异常。<br> 2. 在一次导出完成之后，用户不应删除该表，该表的状态可用于下次导出任务。<br> 3. 该表会开启TTL，数据自动过期，因此可认为其数据量很小。<br> 4. 针对同一个instance下的多个不同的dataTable的Reader配置，可以使用同一个statusTable，记录的状态信息互不影响。 <br> 综上，用户配置一个类似TableStoreStreamReaderStatusTable之类的名称即可，注意不要与业务相关的表重名。| String | 是 |
| startTimestampMillis | 增量数据的时间范围（左闭右开）的左边界，单位毫秒。 <br> 1. Reader插件会从statusTable中找对应startTimestampMillis的位点，从该点开始读取开始导出数据。 <br> 2. 若statusTable中找不到对应的位点，则从系统保留的增量数据的第一条开始读取，并跳过写入时间小于startTimestampMillis的数据。| Long | 否 |
| endTimestampMillis | 增量数据的时间范围（左闭右开）的右边界，单位毫秒。<br> 1. Reader插件从startTimestampMillis位置开始导出数据后，当遇到第一条时间戳大于等于endTimestampMillis的数据时，结束导出数据，导出完成。 <br> 2. 当读取完当前全部的增量数据时，结束读取，即使未达到endTimestampMillis。 |  Long | 否 |
| date | 日期格式为yyyyMMdd，如20151111，表示导出该日的数据。<br> 若没有指定date，则必须指定startTimestampMillis和endTimestampMillis，反之也成立。 | String | 否 |
| isExportSequenceInfo | 是否导出时序信息，时序信息包含了数据的写入时间等。默认该值为false，即不导出。 | Boolean | 否 |
| maxRetries | 从TableStore中读增量数据时，每次请求的最大重试次数，默认为30，重试之间有间隔，30次重试总时间约为5分钟，一般无需更改。| Int | 否 |

### 导出的数据格式
首先，在TableStore多版本模型下，表中的数据组织为“行－列－版本”三级的模式，
一行可以有任意列，列名也并非固定的，每一列可以含有多个版本，每个版本都有一个特定的时间戳（版本号）。

用户可以通过TableStore的API进行一系列读写操作，
TableStore通过记录用户最近对表的一系列写操作（或称为数据更改操作）来实现记录增量数据的目的，
所以也可以把增量数据看作一批操作记录。

TableStore有三类数据更改操作：PutRow、UpdateRow、DeleteRow。

 + PutRow的语义是写入一行，若该行已存在即覆盖该行。

 + UpdateRow的语义是更新一行，对原行其他数据不做更改，
 更新可能包括新增或覆盖（若对应列的对应版本已存在）一些列值、删除某一列的全部版本、删除某一列的某个版本。
 
 + DeleteRow的语义是删除一行。

TableStore会根据每种操作生成对应的增量数据记录，Reader插件会读出这些记录，并导出成Datax的数据格式。

同时，由于TableStore具有动态列、多版本的特性，所以Reader插件导出的一行不对应TableStore中的一行，而是对应TableStore中的一列的一个版本。
即<B>TableStore中的一行可能会导出很多行，每行包含主键值、该列的列名、该列下该版本的时间戳（版本号）、该版本的值、操作类型</B>。若设置isExportSequenceInfo为true，还会包括时序信息。

转换为Datax的数据格式后，我们定义了四种操作类型，分别为:

 + U（UPDATE）: 写入一列的一个版本
 
 + DO（DELETE_ONE_VERSION）: 删除某一列的某个版本
 
 + DA（DELETE_ALL_VERSION）: 删除某一列的全部版本，此时需要根据主键和列名，将对应列的全部版本删除
 
 + DR（DELETE_ROW）: 删除某一行，此时需要根据主键，将该行数据全部删除
 
 
举例如下，假设该表有两个主键列，主键列名分别为pkName1, pkName2：

| pkName1 | pkName2 | columnName | timestamp | columnValue | opType |
| ------- | ------- | ---------- | --------- | ----------- | ------ |
| pk1_V1 | pk2_V1 | col_a | 1441803688001 | col_val1 | U |
| pk1_V1 | pk2_V1 | col_a | 1441803688002 | col_val2 | U |
| pk1_V1 | pk2_V1 | col_b | 1441803688003 | col_val3 | U | 
| pk1_V2 | pk2_V2 | col_a | 1441803688000 | | DO |
| pk1_V2 | pk2_V2 | col_b |  | | DA |
| pk1_V3 | pk2_V3 | |  | | DR |
| pk1_V3 | pk2_V3 | col_a | 1441803688005 | col_val1 | U |
     
假设导出的数据如上，共7行，对应TableStore表内的3行，主键分别是(pk1_V1,pk2_V1), (pk1_V2, pk2_V2), (pk1_V3, pk2_V3)。

对于主键为(pk1_V1, pk2_V1)的一行，包含三个操作，分别是写入col_a列的两个版本和col_b列的一个版本。

对于主键为(pk1_V2, pk2_V2)的一行，包含两个操作，分别是删除col_a列的一个版本、删除col_b列的全部版本。

对于主键为(pk1_V3, pk2_V3)的一行，包含两个操作，分别是删除整行、写入col_a列的一个版本。
