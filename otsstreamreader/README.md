## TableStore增量数据导出通道：TableStoreStreamReader
本文为您介绍OTSStream Reader支持的数据类型、读取方式、字段映射和数据源等参数及配置示例。
## 列模式


### 背景信息

OTSStream Reader插件主要用于导出Table Store的增量数据。您可以将增量数据看作操作日志，除数据本身外还附有操作信息。

与全量导出插件不同，增量导出插件只有多版本模式，且不支持指定列。使用插件前，您必须确保表上已经开启Stream功能。您可以在建表时指定开启，也可以使用SDK的UpdateTable接口开启。

开启Stream的方法，如下所示。
```java
SyncClient client = new SyncClient("", "", "", "");
#建表的时候开启：
CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta);
createTableRequest.setStreamSpecification(new StreamSpecification(true, 24)); // 24代表增量数据保留24小时。
client.createTable(createTableRequest);
#如果建表时未开启，您可以通过UpdateTable开启:
UpdateTableRequest updateTableRequest = new UpdateTableRequest("tableName");
updateTableRequest.setStreamSpecification(new StreamSpecification(true, 24)); 
client.updateTable(updateTableRequest);
```
您使用SDK的UpdateTable功能，指定开启Stream并设置过期时间，即开启了Table Store增量数据导出功能。开启后，Table Store服务端就会将您的操作日志额外保存起来，每个分区有一个有序的操作日志队列，每条操作日志会在一定时间后被垃圾回收，该时间即为您指定的过期时间。

Table Store的SDK提供了几个Stream相关的API用于读取这部分的操作日志，增量插件也是通过Table Store SDK的接口获取到增量数据，默认情况下会将增量数据转化为多个6元组的形式（pk、colName、version、colValue、opType和sequenceInfo）导入至MaxCompute中。

### 列模式

在Table Store多版本模型下，表中的数据组织为行>列>版本三级的模式， 一行可以有任意列，列名并不是固定的，每一列可以含有多个版本，每个版本都有一个特定的时间戳（版本号）。

您可以通过Table Store的API进行一系列读写操作，Table Store通过记录您最近对表的一系列写操作（或数据更改操作）来实现记录增量数据的目的，所以您也可以把增量数据看作一批操作记录。

Table Store支持**PutRow**、**UpdateRow**和**DeleteRow**操作：
- **PutRow**：写入一行，如果该行已存在即覆盖该行。
- **UpdateRow**：更新一行，不更改原行的其它数据。更新包括新增或覆盖（如果对应列的对应版本已存在）一些列值、删除某一列的全部版本、删除某一列的某个版本。
- **DeleteRow**：删除一行。

Table Store会根据每种操作生成对应的增量数据记录，Reader插件会读出这些记录，并导出为数据集成的数据格式。

同时，由于Table Store具有动态列、多版本的特性，所以Reader插件导出的一行不对应Table Store中的一行，而是对应Table Store中的一列的一个版本。即Table Store中的一行可能会导出很多行，每行包含主键值、该列的列名、该列下该版本的时间戳（版本号）、该版本的值、操作类型。如果设置isExportSequenceInfo为true，还会包括时序信息。

转换为数据集成的数据格式后，定义了以下四种操作类型：
- **U（UPDATE）**：写入一列的一个版本。
- **DO（DELETE_ONE_VERSION）**：删除某一列的某个版本。
- **DA（DELETE_ALL_VERSION）**：删除某一列的全部版本，此时需要根据主键和列名，删除对应列的全部版本。
- **DR（DELETE_ROW）**：删除某一行，此时需要根据主键，删除该行数据。

假设该表有两个主键列，主键列名分别为pkName1， pkName2，示例如下。

| **pkName1** | **pkName2** | **columnName** | **timestamp** | **columnValue** | **opType** |
| --- | --- | --- | --- | --- | --- |
| pk1_V1 | pk2_V1 | col_a | 1441803688001 | col_val1 | U |
| pk1_V1 | pk2_V1 | col_a | 1441803688002 | col_val2 | U |
| pk1_V1 | pk2_V1 | col_b | 1441803688003 | col_val3 | U |
| pk1_V2 | pk2_V2 | col_a | 1441803688000 | — | DO |
| pk1_V2 | pk2_V2 | col_b | — | — | DA |
| pk1_V3 | pk2_V3 | — | — | — | DR |
| pk1_V3 | pk2_V3 | col_a | 1441803688005 | col_val1 | U |

假设导出的数据如上，共7行，对应Table Store表内的3行，主键分别是（pk1_V1，pk2_V1），（pk1_V2， pk2_V2），（pk1_V3， pk2_V3）：
- 对于主键为（pk1_V1，pk2_V1）的一行，包括写入col_a列的两个版本和col_b列的一个版本等操作。
- 对于主键为（pk1_V2，pk2_V2）的一行，包括删除col_a列的一个版本和删除col_b列的全部版本等操作。
- 对于主键为（pk1_V3，pk2_V3）的一行，包括删除整行和写入col_a列的一个版本等操作。

### 行模式
#### 宽行表
您可以通过行模式导出数据，该模式将用户每次更新的记录，抽取成行的形式导出，需要设置mode属性并配置列名。
```json
"parameter": {
  #parameter中配置下面三项配置（例如datasource、table等其它配置项照常配置）。
  "mode": "single_version_and_update_only", # 配置导出模式。
  "column":[  #按照需求添加需要导出TableStore中的列，您可以自定义设置配置个数。
          {
             "name": "uid"  #列名示例，可以是主键或属性列。
          },
          {
             "name": "name"  #列名示例，可以是主键或属性列。
          },
  ],
  "isExportSequenceInfo": false, #single_version_and_update_only模式下只能是false。
}
```
#### 时序表
`otsstreamreader`支持导出时序表中的增量数据，当表为时序表时，需要配置的信息如下:
```json
"parameter": {
  #parameter中配置下面四项配置（例如datasource、table等其它配置项照常配置）。
  "mode": "single_version_and_update_only", # 配置导出模式。
  "isTimeseriesTable":"true",  # 配置导出为时序表。
  "column":[  #按照需求添加需要导出TableStore中的列，您可以自定义设置配置个数。
          {
            "name": "_m_name"       #度量名称字段。
          },
          {
            "name": "_data_source"  #数据源字段。
          },
          {
            "name": "_tags"         #标签字段，将tags转换为string类型。
          },
          {
            "name": "tag1_1",       #标签内部字段键名称。
            "is_timeseries_tag":"true"  #表明改字段为tags内部字段。
          },
          {
            "name": "time"          #时间戳字段。
          },
          {
             "name": "name"         #属性列名称。
          },
  ],
  "isExportSequenceInfo": false, #single_version_and_update_only模式下只能是false。
}
```

行模式导出的数据更接近于原始的行，易于后续处理，但需要注意以下问题：
- 每次导出的行是从用户每次更新的记录中抽取，每一行数据与用户的写入或更新操作一一对应。如果用户存在单独更新某些列的行为，则会出现有一些记录只有被更新的部分列，其它列为空的情况。
- 行模式不会导出数据的版本号（即每列的时间戳），也无法进行删除操作。

### 数据类型转换列表
目前OTSStream Reader支持所有的Table Store类型，其针对Table Store类型的转换列表，如下所示。

| **类型分类** | **OTSStream数据类型** |
| --- | --- |
| 整数类 | INTEGER |
| 浮点类 | DOUBLE |
| 字符串类 | STRING |
| 布尔类 | BOOLEAN |
| 二进制类 | BINARY |



### 参数说明

| **参数** | **描述**                                                                                                                                                                                                                                                                                                                                                                                                                                                              | **是否必选** | **默认值** |
| --- |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| --- |---------|
| **dataSource** | 数据源名称，脚本模式支持添加数据源，该配置项填写的内容必须与添加的数据源名称保持一致。                                                                                                                                                                                                                                                                                                                                                                                                                         | 是 | 无       |
| **dataTable** | 导出增量数据的表的名称。该表需要开启Stream，可以在建表时开启，或者使用UpdateTable接口开启。                                                                                                                                                                                                                                                                                                                                                                                                              | 是 | 无       |
| **statusTable** | Reader插件用于记录状态的表的名称，这些状态可用于减少对非目标范围内的数据的扫描，从而加快导出速度。statusTable是Reader用于保存状态的表，如果该表不存在，Reader会自动创建该表。一次离线导出任务完成后，您无需删除该表，该表中记录的状态可用于下次导出任务中：<li> 您无需创建该表，只需要给出一个表名。Reader插件会尝试在您的instance下创建该表，如果该表不存在即创建新表。如果该表已存在，会判断该表的Meta是否与期望一致，如果不一致会抛出异常。<li>在一次导出完成之后，您无需删除该表，该表的状态可以用于下次的导出任务。<li> 该表会开启TTL，数据自动过期，会认为其数据量很小。<li> 针对同一个instance下的多个不同的dataTable的Reader配置，可以使用同一个statusTable，记录的状态信息互不影响。您配置一个类似**TableStoreStreamReaderStatusTable**的名称即可，请注意不要与业务相关的表重名。 | 是 | 无       |
| **startTimestampMillis** | 增量数据的时间范围（左闭右开）的左边界，单位为毫秒: <li> Reader插件会从statusTable中找对应**startTimestampMillis**的位点，从该点开始读取开始导出数据。<li> 如果statusTable中找不到对应的位点，则从系统保留的增量数据的第一条开始读取，并跳过写入时间小于**startTimestampMillis**的数据。                                                                                                                                                                                                                                                                            | 否 | 无       |
| **endTimestampMillis** | 增量数据的时间范围（左闭右开）的右边界，单位为毫秒：<li> Reader插件从**startTimestampMillis**位置开始导出数据后，当遇到第一条时间戳大于等于**endTimestampMillis**的数据时，结束导出数据，导出完成。<li> 当读取完当前全部的增量数据时，即使未达到**endTimestampMillis**，也会结束读取。                                                                                                                                                                                                                                                                               | 否 | 无       |
| **date** | 日期格式为**yyyyMMdd**，例如20151111，表示导出该日的数据。如果没有指定**date**，则需要指定**startTimestampMillis**和**endTimestampMillis**或**startTimeString**和**endTimeString**，反之也成立。例如，采云间调度仅支持天级别，所以提供该配置，作用与**startTimestampMillis**和**endTimestampMillis**或**startTimeString**和**endTimeString**类似。                                                                                                                                                                                           | 否 | 无       |
| **isExportSequenceInfo** | 是否导出时序信息，时序信息包含了数据的写入时间等。默认该值为false，即不导出。                                                                                                                                                                                                                                                                                                                                                                                                                           | 否 | false   |
| **maxRetries** | 从TableStore中读增量数据时，每次请求的最大重试次数，默认为30次。重试之间有间隔，重试30次的总时间约为5分钟，通常无需更改。                                                                                                                                                                                                                                                                                                                                                                                                | 否 | 30      |
| **startTimeString** | 任务的开始时间，即增量数据的时间范围（左闭右开）的左边界，格式为**yyyymmddhh24miss**，单位为秒。                                                                                                                                                                                                                                                                                                                                                                                                          | 否 | 无       |
| **endTimeString** | 任务的结束时间，即增量数据的时间范围（左闭右开）的右边界，格式为**yyyymmddhh24miss**，单位为秒。                                                                                                                                                                                                                                                                                                                                                                                                          | 否 | 无       |
| **enableSeekIterator** | Reader插件需要先确定增量位点，然后再拉取数据，如果是经常运行的任务，插件会根据之前扫描的位点来确定位置。如果之前没运行过这个插件，将会从增量开始位置（默认增量保留7天，即7天前）开始扫描，因此当还没有扫描到设置的开始时间之后的数据时，会存在开始一段时间没有数据导出的情况，您可以在reader的配置参数里增加** "enableSeekIterator": true**的配置，帮助您加快位点定位。                                                                                                                                                                                                                                                          | 否 | false   |
| **mode** | 导出模式，设置为**single_version_and_update_only**时为行模式，默认不设置为列模式。                                                                                                                                                                                                                                                                                                                                                                                                          | 否 | 无       |
| **isTimeseriesTable** | 是否为时序表，只有在行模式，即**mode**为**single_version_and_update_only**时配置生效。                                                                                                                                                                                                                                                                                                                                                                                           | 否 | false   |



