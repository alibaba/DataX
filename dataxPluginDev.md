# DataX插件开发宝典

本文面向DataX插件开发人员，尝试尽可能全面地阐述开发一个DataX插件所经过的历程，力求消除开发者的困惑，让插件开发变得简单。

## 一、开发之前

>  路走对了，就不怕远。✓
>  路走远了，就不管对不对。✕

当你打开这篇文档，想必已经不用在此解释什么是`DataX`了。那下一个问题便是：

###  `DataX`为什么要使用插件机制？

从设计之初，`DataX`就把异构数据源同步作为自身的使命，为了应对不同数据源的差异、同时提供一致的同步原语和扩展能力，`DataX`自然而然地采用了`框架` + `插件` 的模式：

- 插件只需关心数据的读取或者写入本身。
- 而同步的共性问题，比如：类型转换、性能、统计，则交由框架来处理。

作为插件开发人员，则需要关注两个问题：

1. 数据源本身的读写数据正确性。
2. 如何与框架沟通、合理正确地使用框架。

###  开工前需要想明白的问题

就插件本身而言，希望在您动手coding之前，能够回答我们列举的这些问题，不然路走远了发现没走对，就尴尬了。

## 二、插件视角看框架

### 逻辑执行模型

插件开发者不用关心太多，基本只需要关注特定系统读和写，以及自己的代码在逻辑上是怎样被执行的，哪一个方法是在什么时候被调用的。在此之前，需要明确以下概念：

- `Job`: `Job`是DataX用以描述从一个源头到一个目的端的同步作业，是DataX数据同步的最小业务单元。比如：从一张mysql的表同步到odps的一个表的特定分区。
- `Task`: `Task`是为最大化而把`Job`拆分得到的最小执行单元。比如：读一张有1024个分表的mysql分库分表的`Job`，拆分成1024个读`Task`，用若干个并发执行。
- `TaskGroup`:  描述的是一组`Task`集合。在同一个`TaskGroupContainer`执行下的`Task`集合称之为`TaskGroup`
- `JobContainer`:  `Job`执行器，负责`Job`全局拆分、调度、前置语句和后置语句等工作的工作单元。类似Yarn中的JobTracker
- `TaskGroupContainer`: `TaskGroup`执行器，负责执行一组`Task`的工作单元，类似Yarn中的TaskTracker。

简而言之， **`Job`拆分成`Task`，在分别在框架提供的容器中执行，插件只需要实现`Job`和`Task`两部分逻辑**。

### 物理执行模型

框架为插件提供物理上的执行能力（线程）。`DataX`框架有三种运行模式：

- `Standalone`: 单进程运行，没有外部依赖。
- `Local`: 单进程运行，统计信息、错误信息汇报到集中存储。
- `Distrubuted`: 分布式多进程运行，依赖`DataX Service`服务。

当然，上述三种模式对插件的编写而言没有什么区别，你只需要避开一些小错误，插件就能够在单机/分布式之间无缝切换了。
当`JobContainer`和`TaskGroupContainer`运行在同一个进程内时，就是单机模式（`Standalone`和`Local`）；当它们分布在不同的进程中执行时，就是分布式（`Distributed`）模式。

是不是很简单？

### 编程接口

那么，`Job`和`Task`的逻辑应是怎么对应到具体的代码中的？

首先，插件的入口类必须扩展`Reader`或`Writer`抽象类，并且实现分别实现`Job`和`Task`两个内部抽象类，`Job`和`Task`的实现必须是 **内部类** 的形式，原因见 **加载原理** 一节。以Reader为例：

```java
public class SomeReader extends Reader {
    public static class Job extends Reader.Job {

        @Override
        public void init() {
        }
		
		@Override
		public void prepare() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return null;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

    }

    public static class Task extends Reader.Task {

        @Override
        public void init() {
        }
		
		@Override
		public void prepare() {
        }

        @Override
        public void startRead(RecordSender recordSender) {
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }
}
```

`Job`接口功能如下：
- `init`: Job对象初始化工作，测试可以通过`super.getPluginJobConf()`获取与本插件相关的配置。读插件获得配置中`reader`部分，写插件获得`writer`部分。
- `prepare`: 全局准备工作，比如odpswriter清空目标表。
- `split`: 拆分`Task`。参数`adviceNumber`框架建议的拆分数，一般是运行时所配置的并发度。值返回的是`Task`的配置列表。
- `post`: 全局的后置工作，比如mysqlwriter同步完影子表后的rename操作。
- `destroy`: Job对象自身的销毁工作。

`Task`接口功能如下：
- `init`：Task对象的初始化。此时可以通过`super.getPluginJobConf()`获取与本`Task`相关的配置。这里的配置是`Job`的`split`方法返回的配置列表中的其中一个。
- `prepare`：局部的准备工作。
- `startRead`: 从数据源读数据，写入到`RecordSender`中。`RecordSender`会把数据写入连接Reader和Writer的缓存队列。
- `startWrite`：从`RecordReceiver`中读取数据，写入目标数据源。`RecordReceiver`中的数据来自Reader和Writer之间的缓存队列。
- `post`: 局部的后置工作。
- `destroy`: Task象自身的销毁工作。

需要注意的是：
- `Job`和`Task`之间一定不能有共享变量，因为分布式运行时不能保证共享变量会被正确初始化。两者之间只能通过配置文件进行依赖。
- `prepare`和`post`在`Job`和`Task`中都存在，插件需要根据实际情况确定在什么地方执行操作。

框架按照如下的顺序执行`Job`和`Task`的接口：

![DataXReaderWriter (2)](https://github.com/alibaba/DataX/blob/master/images/plugin_dev_guide_1.png)

上图中，黄色表示`Job`部分的执行阶段，蓝色表示`Task`部分的执行阶段，绿色表示框架执行阶段。

相关类关系如下：

![DataX](https://github.com/alibaba/DataX/blob/master/images/plugin_dev_guide_2.png)

### 插件定义

代码写好了，有没有想过框架是怎么找到插件的入口类的？框架是如何加载插件的呢？

在每个插件的项目中，都有一个`plugin.json`文件，这个文件定义了插件的相关信息，包括入口类。例如：

```json
{
    "name": "mysqlwriter",
    "class": "com.alibaba.datax.plugin.writer.mysqlwriter.MysqlWriter",
    "description": "Use Jdbc connect to database, execute insert sql.",
    "developer": "alibaba"
}
```

- `name`: 插件名称，大小写敏感。框架根据用户在配置文件中指定的名称来搜寻插件。 **十分重要** 。
- `class`: 入口类的全限定名称，框架通过反射穿件入口类的实例。**十分重要** 。
- `description`: 描述信息。
- `developer`: 开发人员。

### 打包发布

`DataX`使用`assembly`打包，`assembly`的使用方法请咨询谷哥或者度娘。打包命令如下：

```bash
mvn clean package -DskipTests assembly:assembly
```

`DataX`插件需要遵循统一的目录结构：

```
${DATAX_HOME}
|-- bin       
|   `-- datax.py
|-- conf
|   |-- core.json
|   `-- logback.xml
|-- lib
|   `-- datax-core-dependencies.jar
`-- plugin
    |-- reader
    |   `-- mysqlreader
    |       |-- libs
    |       |   `-- mysql-reader-plugin-dependencies.jar
    |       |-- mysqlreader-0.0.1-SNAPSHOT.jar
    |       `-- plugin.json
    `-- writer
        |-- mysqlwriter
        |   |-- libs
        |   |   `-- mysql-writer-plugin-dependencies.jar
        |   |-- mysqlwriter-0.0.1-SNAPSHOT.jar
        |   `-- plugin.json
        |-- oceanbasewriter
        `-- odpswriter
```

- `${DATAX_HOME}/bin`:  可执行程序目录。
- `${DATAX_HOME}/conf`:  框架配置目录。
- `${DATAX_HOME}/lib`:  框架依赖库目录。
- `${DATAX_HOME}/plugin`:  插件目录。

插件目录分为`reader`和`writer`子目录，读写插件分别存放。插件目录规范如下：

- `${PLUGIN_HOME}/libs`: 插件的依赖库。
- `${PLUGIN_HOME}/plugin-name-version.jar`: 插件本身的jar。
- `${PLUGIN_HOME}/plugin.json`: 插件描述文件。

尽管框架加载插件时，会把`${PLUGIN_HOME}`下所有的jar放到`classpath`，但还是推荐依赖库的jar和插件本身的jar分开存放。

注意：
**插件的目录名字必须和`plugin.json`中定义的插件名称一致。**

### 配置文件

`DataX`使用`json`作为配置文件的格式。一个典型的`DataX`任务配置如下：

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "odpsreader",
          "parameter": {
            "accessKey": "",
            "accessId": "",
            "column": [""],
            "isCompress": "",
            "odpsServer": "",
            "partition": [
              ""
            ],
            "project": "",
            "table": "",
            "tunnelServer": ""
          }
        },
        "writer": {
          "name": "oraclewriter",
          "parameter": {
            "username": "",
            "password": "",
            "column": ["*"],
            "connection": [
              {
                "jdbcUrl": "",
                "table": [
                  ""
                ]
              }
            ]
          }
        }
      }
    ]
  }
}
```

`DataX`框架有`core.json`配置文件，指定了框架的默认行为。任务的配置里头可以指定框架中已经存在的配置项，而且具有更高的优先级，会覆盖`core.json`中的默认值。

**配置中`job.content.reader.parameter`的value部分会传给`Reader.Job`；`job.content.writer.parameter`的value部分会传给`Writer.Job`** ，`Reader.Job`和`Writer.Job`可以通过`super.getPluginJobConf()`来获取。

`DataX`框架支持对特定的配置项进行RSA加密，例子中以`*`开头的项目便是加密后的值。 **配置项加密解密过程对插件是透明，插件仍然以不带`*`的key来查询配置和操作配置项** 。

#### 如何设计配置参数

> 配置文件的设计是插件开发的第一步！

任务配置中`reader`和`writer`下`parameter`部分是插件的配置参数，插件的配置参数应当遵循以下原则：

- 驼峰命名：所有配置项采用驼峰命名法，首字母小写，单词首字母大写。
- 正交原则：配置项必须正交，功能没有重复，没有潜规则。
- 富类型：合理使用json的类型，减少无谓的处理逻辑，减少出错的可能。
    - 使用正确的数据类型。比如，bool类型的值使用`true`/`false`，而非`"yes"`/`"true"`/`0`等。
    - 合理使用集合类型，比如，用数组替代有分隔符的字符串。
- 类似通用：遵守同一类型的插件的习惯，比如关系型数据库的`connection`参数都是如下结构：

    ```json
    {
      "connection": [
        {
          "table": [
            "table_1",
            "table_2"
          ],
          "jdbcUrl": [
            "jdbc:mysql://127.0.0.1:3306/database_1",
            "jdbc:mysql://127.0.0.2:3306/database_1_slave"
          ]
        },
        {
          "table": [
            "table_3",
            "table_4"
          ],
          "jdbcUrl": [
            "jdbc:mysql://127.0.0.3:3306/database_2",
            "jdbc:mysql://127.0.0.4:3306/database_2_slave"
          ]
        }
      ]
    }
    ``` 
- ...

#### 如何使用`Configuration`类

为了简化对json的操作，`DataX`提供了简单的DSL配合`Configuration`类使用。

`Configuration`提供了常见的`get`, `带类型get`，`带默认值get`，`set`等读写配置项的操作，以及`clone`, `toJSON`等方法。配置项读写操作都需要传入一个`path`做为参数，这个`path`就是`DataX`定义的DSL。语法有两条：

1. 子map用`.key`表示，`path`的第一个点省略。
2. 数组元素用`[index]`表示。

比如操作如下json：

```json
{
  "a": {
    "b": {
      "c": 2
    },
    "f": [
      1,
      2,
      {
        "g": true,
        "h": false
      },
      4
    ]
  },
  "x": 4
}
```

比如调用`configuration.get(path)`方法，当path为如下值的时候得到的结果为：

- `x`：`4`
- `a.b.c`：`2`
- `a.b.c.d`：`null`
- `a.b.f[0]`：`1`
- `a.b.f[2].g`：`true`

注意，因为插件看到的配置只是整个配置的一部分。使用`Configuration`对象时，需要注意当前的根路径是什么。

更多`Configuration`的操作请参考`ConfigurationTest.java`。

### 插件数据传输

跟一般的`生产者-消费者`模式一样，`Reader`插件和`Writer`插件之间也是通过`channel`来实现数据的传输的。`channel`可以是内存的，也可能是持久化的，插件不必关心。插件通过`RecordSender`往`channel`写入数据，通过`RecordReceiver`从`channel`读取数据。


`channel`中的一条数据为一个`Record`的对象，`Record`中可以放多个`Column`对象，这可以简单理解为数据库中的记录和列。

`Record`有如下方法：

```java
public interface Record {
    // 加入一个列，放在最后的位置
    void addColumn(Column column);
    // 在指定下标处放置一个列
    void setColumn(int i, final Column column);
    // 获取一个列
    Column getColumn(int i);
    // 转换为json String
    String toString();
    // 获取总列数
    int getColumnNumber();
    // 计算整条记录在内存中占用的字节数
    int getByteSize();
}
```

因为`Record`是一个接口，`Reader`插件首先调用`RecordSender.createRecord()`创建一个`Record`实例，然后把`Column`一个个添加到`Record`中。

`Writer`插件调用`RecordReceiver.getFromReader()`方法获取`Record`，然后把`Column`遍历出来，写入目标存储中。当`Reader`尚未退出，传输还在进行时，如果暂时没有数据`RecordReceiver.getFromReader()`方法会阻塞直到有数据。如果传输已经结束，会返回`null`，`Writer`插件可以据此判断是否结束`startWrite`方法。

`Column`的构造和操作，我们在《类型转换》一节介绍。

### 类型转换

为了规范源端和目的端类型转换操作，保证数据不失真，DataX支持六种内部数据类型：

- `Long`：定点数(Int、Short、Long、BigInteger等)。
- `Double`：浮点数(Float、Double、BigDecimal(无限精度)等)。
- `String`：字符串类型，底层不限长，使用通用字符集(Unicode)。
- `Date`：日期类型。
- `Bool`：布尔值。
- `Bytes`：二进制，可以存放诸如MP3等非结构化数据。


对应地，有`DateColumn`、`LongColumn`、`DoubleColumn`、`BytesColumn`、`StringColumn`和`BoolColumn`六种`Column`的实现。


`Column`除了提供数据相关的方法外，还提供一系列以`as`开头的数据类型转换转换方法。

![Columns](https://github.com/alibaba/DataX/blob/master/images/plugin_dev_guide_3.png)


DataX的内部类型在实现上会选用不同的java类型：

| 内部类型 | 实现类型 | 备注 |
| ----- | -------- | ----- |
| Date  | java.util.Date |     |
| Long  | java.math.BigInteger|  使用无限精度的大整数，保证不失真   |
| Double| java.lang.String| 用String表示，保证不失真 |
| Bytes | byte[]|  |
| String|  java.lang.String   |     |
| Bool  | java.lang.Boolean   | |

类型之间相互转换的关系如下：

| from\to     |   Date  |  Long  | Double | Bytes   | String | Bool   |
| -----   | -------- | ----- | ------ | -------- | ----- |  ----- |
| Date    |    -     |  使用毫秒时间戳  |   不支持  |    不支持      |   使用系统配置的date/time/datetime格式转换    |  不支持  |
| Long    |  作为毫秒时间戳构造Date    |   -   | BigInteger转为BigDecimal，然后BigDecimal.doubleValue()       |    不支持      |  BigInteger.toString()    | 0为false，否则true   |
| Double  |  不支持   | 内部String构造BigDecimal，然后BigDecimal.longValue()   |    -   |     不支持     |  直接返回内部String    |        |
| Bytes   |  不支持   | 不支持 | 不支持  |    -     |  按照`common.column.encoding`配置的编码转换为String，默认`utf-8`  |  不支持  |
| String  | 按照配置的date/time/datetime/extra格式解析 |  用String构造BigDecimal，然后取longValue()  |   用String构造BigDecimal，然后取doubleValue(),会正确处理`NaN`/`Infinity`/`-Infinity`  |   按照`common.column.encoding`配置的编码转换为byte[]，默认`utf-8`     |    -  |    "true"为`true`, "false"为`false`，大小写不敏感。其他字符串不支持    |
| Bool    |    不支持  |  `true`为`1L`，否则`0L`     |        | `true`为`1.0`，否则`0.0`   |  不支持  |    -   |


### 脏数据处理

#### 什么是脏数据？

目前主要有三类脏数据：

1. Reader读到不支持的类型、不合法的值。
1. 不支持的类型转换，比如：`Bytes`转换为`Date`。
2. 写入目标端失败，比如：写mysql整型长度超长。

#### 如何处理脏数据

在`Reader.Task`和`Writer.Task`中，功过`AbstractTaskPlugin.getPluginCollector()`可以拿到一个`TaskPluginCollector`，它提供了一系列`collectDirtyRecord`的方法。当脏数据出现时，只需要调用合适的`collectDirtyRecord`方法，把被认为是脏数据的`Record`传入即可。

用户可以在任务的配置中指定脏数据限制条数或者百分比限制，当脏数据超出限制时，框架会结束同步任务，退出。插件需要保证脏数据都被收集到，其他工作交给框架就好。

### 加载原理


1. 框架扫描`plugin/reader`和`plugin/writer`目录，加载每个插件的`plugin.json`文件。
2. 以`plugin.json`文件中`name`为key，索引所有的插件配置。如果发现重名的插件，框架会异常退出。
3. 用户在插件中在`reader`/`writer`配置的`name`字段指定插件名字。框架根据插件的类型（`reader`/`writer`）和插件名称去插件的路径下扫描所有的jar，加入`classpath`。
4. 根据插件配置中定义的入口类，框架通过反射实例化对应的`Job`和`Task`对象。


## 三、Last but not Least

> 文档是工程师的良知。

每个插件都必须在`DataX`官方wiki中有一篇文档，文档需要包括但不限于以下内容：

1. **快速介绍**：介绍插件的使用场景，特点等。
2. **实现原理**：介绍插件实现的底层原理，比如`mysqlwriter`通过`insert into`和`replace into`来实现插入，`tair`插件通过tair客户端实现写入。
3. **配置说明**
	- 给出典型场景下的同步任务的json配置文件。
	- 介绍每个参数的含义、是否必选、默认值、取值范围和其他约束。
4. **类型转换**
    - 插件是如何在实际的存储类型和`DataX`的内部类型之间进行转换的。
    - 以及是否存在特殊处理。
5. **性能报告**
	- 软硬件环境，系统版本，java版本，CPU、内存等。
	- 数据特征，记录大小等。
	- 测试参数集（多组），系统参数（比如并发数），插件参数（比如batchSize）
	- 不同参数下同步速度（Rec/s, MB/s），机器负载（load, cpu）等，对数据源压力（load, cpu, mem等）。
6. **约束限制**：是否存在其他的使用限制条件。
7. **FQA**：用户经常会遇到的问题。
