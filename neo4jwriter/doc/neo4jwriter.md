# DataX neo4jWriter 插件文档

## 功能简介

本目前市面上的neo4j 批量导入主要有Cypher Create,Load CSV,第三方或者官方提供的Batch Import。Load CSV支持节点10W级别一下，Batch Import 需要对数据库进行停机。要想实现不停机的数据写入，Cypher是最好的方式。

## 支持版本

支持Neo4j 4 和Neo4j 5,如果是Neo4j 3,需要自行将驱动降低至相对应的版本进行编译。

## 实现原理

将datax的数据转换成了neo4j驱动能识别的对象，利用 unwind 语法进行批量插入。

## 如何配置

### 配置项介绍

| 配置                             | 说明                 | 是否必须 | 默认值    | 示例                                                   |
|:-------------------------------|--------------------| -------- |--------|------------------------------------------------------|
| database                       | 数据库名字              | 是       | -      | neo4j                                                |
| uri                            | 数据库访问链接            | 是       | -      | bolt://localhost:7687                                |
| username                       | 访问用户名              | 是       | -      | neo4j                                                |
| password                       | 访问密码               | 是       | -      | neo4j                                                |
| bearerToken                    | 权限相关               | 否       | -      | -                                                    |
| kerberosTicket                 | 权限相关               | 否       | -      | -                                                    |
| cypher                         | 同步语句               | 是       | -      | unwind $batch as row create(p) set p.name = row.name |
| batchDataVariableName          | unwind 携带的数据变量名    |          | batch  | batch                                                |
| properties                     | 定义neo4j中数据的属性名字和类型 | 是       | -      | 见后续案例                                                |
| batchSize                      | 一批写入数据量            | 否       | 1000   |                                                      |
| maxTransactionRetryTimeSeconds | 事务运行最长时间           | 否       | 30秒    | 30                                                   |
| maxConnectionTimeoutSeconds    | 驱动最长链接时间           | 否       | 30秒    | 30                                                   |
| retryTimes                     | 发生错误的重试次数          | 否       | 3次     | 3                                                    |
| retrySleepMills                | 重试失败后的等待时间         | 否       | 3秒     | 3                                                    |
| writeMode                      | 写入模式               | 否       | INSERT | INSERT  or UPDATE                                    |

### 支持的数据类型
> 配置时均忽略大小写
```
BOOLEAN, 
STRING,
LONG,
SHORT,
INTEGER,
DOUBLE,
FLOAT,
LOCAL_DATE,
LOCAL_TIME,
LOCAL_DATE_TIME,
LIST,
//map类型支持 . 属性表达式取值
MAP,
CHAR_ARRAY,
BYTE_ARRAY,
BOOLEAN_ARRAY,
STRING_ARRAY,
LONG_ARRAY,
INT_ARRAY,
SHORT_ARRAY,
DOUBLE_ARRAY,
FLOAT_ARRAY,
Object_ARRAY
```

### 写节点

这里提供了一个写节点包含很多类型属性的例子。你可以在我的测试方法中运行。

```json
"writer": {
        "name": "neo4jWriter",
        "parameter": {
            "uri": "neo4j://localhost:7687",
            "username": "neo4j",
            "password": "Test@12343",
            "database": "neo4j",
            "cypher": "unwind $batch as row create(p:Person) set p.pbool = row.pbool,p.pstring = row.pstring,p.plong = row.plong,p.pshort = row.pshort,p.pdouble=row.pdouble,p.pstringarr=row.pstringarr,p.plocaldate=row.plocaldate",
            "batchDataVariableName": "batch",
            "batchSize": "33",
            "properties": [
                {
                    "name": "pbool",
                    "type": "BOOLEAN"
                },
                {
                    "name": "pstring",
                    "type": "STRING"
                },
                {
                    "name": "plong",
                    "type": "LONG"
                },
                {
                    "name": "pshort",
                    "type": "SHORT"
                },
                {
                    "name": "pdouble",
                    "type": "DOUBLE"
                },
                {
                    "name": "pstringarr",
                    "type": "STRING_ARRAY",
                    "split": ","
                },
                {
                    "name": "plocaldate",
                    "type": "LOCAL_DATE",
                    "dateFormat": "yyyy-MM-dd"
                }
            ]
        }
    }
```

### 写关系

```json
"writer": {
        "name": "neo4jWriter",
        "parameter": {
            "uri": "neo4j://localhost:7687",
            "username": "neo4j",
            "password": "Test@12343",
            "database": "neo4j",
            "cypher": "unwind $batch as row match(p1:Person) where p1.id = row.startNodeId match(p2:Person) where p2.id = row.endNodeId create (p1)-[:LINK]->(p2)",
            "batchDataVariableName": "batch",
            "batchSize": "33",
            "properties": [
                {
                    "name": "startNodeId",
                    "type": "STRING"
                },
                {
                    "name": "endNodeId",
                    "type": "STRING"
                }
            ]
        }
    }
```

### 节点/关系类型动态写

> 需要使用AOPC函数拓展，如果你的数据库没有，请安装APOC函数拓展

```json
    "writer": {
        "name": "neo4jWriter",
        "parameter": {
            "uri": "bolt://localhost:7687",
            "username": "yourUserName",
            "password": "yourPassword",
            "database": "yourDataBase",
            "cypher": "unwind $batch as row CALL apoc.cypher.doIt( 'create (n:`' + row.Label + '`{id:$id})' ,{id: row.id} ) YIELD value RETURN 1 ",
            "batchDataVariableName": "batch",
            "batchSize": "1",
            "properties": [
                {
                    "name": "Label",
                    "type": "STRING"
                },
                {
                    "name": "id",
                    "type": "STRING"
                }
            ]
        }
    }
```
> 同步数据时，每一条数据的Label均不相同的情况下，写入到neo4j似乎有点麻烦。因为cypher语句中 Label 不支持动态引用变量，不得不使用字符串拼接 关系或者节点的 Label.

假设现在同步节点，然后同步关系。

**节点源头表**

| 类型(TYPE) | 姓名属性(NAME) | uid属性(UID) |
| ---------- | -------------- | ------------ |
| Boy        | 小付           | 1            |
| Girl       | 小杰           | 2            |

假设以上两条数据,是节点数据，他们的 Label 分别是 Boy 和 Girl.

那么我们的writer这样配置。

```json
    "writer": {
        "name": "neo4jWriter",
        "parameter": {
            "uri": "bolt://localhost:7687",
            "username": "yourUserName",
            "password": "yourPassword",
            "database": "yourDataBase",
            "cypher": "unwind $batch as row CALL apoc.cypher.doIt( 'create (n:`' + row.type + '`{uid:$uid}) set n.name = name' ,{uid: row.uid,name:row.name,type:row.type} ) YIELD value RETURN 1",
            "batchDataVariableName": "batch",
            "batchSize": "1",
            "properties": [
                {
                    "name": "type",
                    "type": "STRING"
                },
                {
                    "name": "name",
                    "type": "STRING"
                },
                {
                    "name":"uid",
                    "type":"STRING"
                }
            ]
        }
    }
//注意字符串拼接的规则。
前面的语句`'+要拼接的类型+'`后面的语句.
```

我们将每一行的属性都作为参数传递给了apoc函数，在使用类型的地方，使用了字符串拼接。注意字符串拼接的规则。

实际上，以上语句最后到neo4j会被解析如下：

```cypher
unwind [{type:'Boy',uid:'1',name:'小付'},{type:'Girl',uid:'2',name:'小杰'}] as row 
 CALL apoc.cypher.doIt( 'create (n:`' + row.type + '`{uid:$uid}) set n.name = name' ,{uid: row.uid,name:row.name,type:row.type} ) YIELD value RETURN 1
```

假设节点同步成功后，我们开始同步关系。

**关系源头描述表**

| 开始节点id | 结束节点id | 关系id | 开始节点类型type | 结束节点类型type | 关系类型type | 关系属性name |
| ---------- | ---------- | ------ | ---------------- | ---------------- | ------------ | ------------ |
| 1          | 2          | 3      | Boy              | Girl             | Link         | link         |

我们根据开始节点和结束节点建立起连接关系。

```json
    "writer": {
        "name": "neo4jWriter",
        "parameter": {
            "uri": "bolt://localhost:7687",
            "username": "yourUserName",
            "password": "yourPassword",
            "database": "yourDataBase",
            "cypher": "unwind $batch as row CALL apoc.cypher.doIt(
'match(start:`'+row.startType+'`) where start.uid = $startId
match(end:`'+row.endType+'`{uid:$endId}) create (start)-[r:`'+row.rType+'`]->
(end) set r.rid = $rid,r.name=name' ,
{rType:row.rType,startType:row.startType,endType:row.endType,startId:row.startId
,endId:row.endId,name:row.name,rid:row.rid} ) YIELD value RETURN 1",
            "batchDataVariableName": "batch",
            "batchSize": "1000",
            "properties": [
                {
                    "name": "rType",
                    "type": "STRING"
                },
                {
                    "name": "startType",
                    "type": "STRING"
                },
                {
                    "name":"endType",
                    "type":"STRING"
                },
              	{
                  "name":"startId",
                  "type":"STRING"
                },
              	{
                "name":"endId",
                 "type":"STRING"
                },
              	{
                "name":"name",
                 "type":"STRING"
                }
            ]
        }
    }
//注意字符串拼接的规则。
前面的语句`'+要拼接的类型+'`后面的语句.
```

在配置中，我们解析每一行的数据，根据类型和id找到开始节点和结束节点，并将他们链接起来。

实际的cypher会被解析为：

```cypher
unwind
[{rType:'Link',startType:'Boy',endType:'Girl',startId:'1',endId:'2',
name:'link',rid:'3'}] as row
CALL apoc.cypher.doIt( 'match(start:`'+row.startType+'`) where
start.uid = $startId match(end:`'+row.endType+'`{uid:$endId}) create (start)-
[r:`'+row.rType+'`]->(end) set r.rid = $rid,r.name=name' ,
{rType:row.rType,startType:row.startType,endType:row.endType,startId:row.startId
,endId:row.endId,name:row.name,rid:row.rid} ) YIELD value RETURN 1
```

* 动态写入Label的语法确实比较复杂，请用户复制以上案例到测试环境方便理解为何要使用字符串拼接。
* 如果觉得这种写法太过于复杂，后续可能会引入其他方式。

## 注意事项

* properties定义的顺序需要与reader端顺序一一对应。
* 灵活使用map类型，可以免去很多数据加工的烦恼。在cypher中，可以根据 . 属性访问符号一直取值。比如 unwind $batch as row create (p) set p.name = row.prop.name,set p.age = row.prop.age，在这个例子中，prop是map类型，包含name和age两个属性。
* 如果提示事务超时，建议调大事务运行时间或者调小batchSize
* 如果用于更新场景，遇到死锁问题影响写入，建议二开源码加入死锁异常检测，并进行重试。

## 性能报告

**JVM参数**

16G G1垃圾收集器 8核心

**Neo4j数据库配置**

32核心，256G

**datax 配置**

* Channel 20 batchsize = 1000
* 任务平均流量：15.23MB/s
* 记录写入速度：44440 rec/s
* 读出记录总数：2222013
