# DataX ADB PG Writer



---


## 1 快速介绍
AdbpgWriter 插件实现了写入数据到 ABD PG版数据库的功能。在底层实现上，AdbpgWriter 插件会先缓存需要写入的数据，当缓存的
数据量达到 commitSize 时，插件会通过 JDBC 连接远程 ADB PG版 数据库，并执行 COPY 命令将数据写入 ADB PG 数据库。

AdbpgWriter 可以作为数据迁移工具为用户提供服务。

## 2 实现原理

AdbpgWriter 通过 DataX 框架获取 Reader 生成的协议数据，首先会将数据缓存，当缓存的数据量达到commitSize时，插件根据你配置生成相应的COPY语句，执行
COPY命令将数据写入ADB PG数据库中。

## 3 功能说明

### 3.1 配置样例

* 这里使用一份从内存产生到 AdbpgWriter导入的数据

```json

{
  "job": {
    "setting": {
       "speed": {
          "channel": 32
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
                  ]
              },
              "sliceRecordCount": 1000
            },
            
            "writer": {
              "name": "adbpgwriter",
              "parameter": {
                  "username": "",
                  "password": "",
                  "host": "127.0.0.1",
                  "port": "1234",
                  "database": "database",
                  "schema": "schema",
                  "table": "table",
                  "preSql": ["delete * from table"],
                  "postSql": ["select * from table"],
                  "column": ["*"]
              }
            }
         }
      ]
  }
}
```

### 3.2 参数说明

* **name**
  * 描述：插件名称 <br />

  * 必选：是 <br />

  * 默认值：无 <br />

* **username**
  * 描述：目的数据库的用户名 <br />

  * 必选：是 <br />

  * 默认值：无 <br />
 
* **password**

  * 描述：目的数据库的密码 <br />

  * 必选：是 <br />

  * 默认值：无 <br />
  
* **host**

  * 描述：目的数据库主机名 <br />

  * 必选：是 <br />

  * 默认值：无 <br />
  
* **port**
 
   * 描述：目的数据库的端口 <br />
 
   * 必选：是 <br />
 
   * 默认值：无 <br />
* **database**

  * 描述：需要写入的表所属的数据库名称 <br />

  * 必选：是 <br />

  * 默认值：无 <br />
* **schema**

  * 描述：需要写入的表所属的schema名称 <br />

  * 必选：是 <br />

  * 默认值：无 <br />
* **table**

  * 描述：需要写入的表名称 <br />

  * 必选：是 <br />

  * 默认值：无 <br />
* **column**

  * 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。如果要依次写入全部列，使用*表示, 例如: "column": ["*"]

               注意：1、我们强烈不推荐你这样配置，因为当你目的表字段个数、类型等有改动时，你的任务可能运行不正确或者失败
                    2、此处 column 不能配置任何常量值
                    3、大写字段名,此处配置时,不需要拼接转义符号:\"

  * 必选：是 <br />

  * 默认值：否 <br />
* **preSql**
 
   * 描述：写入数据到目的表前，会先执行这里的标准语句。如果 Sql 中有你需要操作到的表名称，可以使用 `@table` 表示，这样在实际执行 Sql 语句时，会对变量按照实际表名称进行替换。比如你的任务是要写入到目的端的100个同构分表(表名称为:datax_00,datax01, ... datax_98,datax_99)，并且你希望导入数据前，先对表中数据进行删除操作，那么你可以这样配置：`"preSql":["delete from @table"]`，效果是：在执行到每个表写入数据前，会先执行对应的 delete from 对应表名称 <br />
 
   * 必选：否 <br />
 
   * 默认值：否 <br />
   
* **postSql**
 
   * 描述：写入数据到目的表后，会先执行这里的标准语句。如果 Sql 中有你需要操作到的表名称，可以使用 `@table` 表示，这样在实际执行 Sql 语句时，会对变量按照实际表名称进行替换。 <br />
 
   * 必选：否 <br />
 
   * 默认值：否 <br />
### 3.3 类型转换

目前 AdbpgWriter 支持大部分 ADB PG 数据库的类型，但也存在部分没有支持的情况，请注意检查你的类型。

下面列出 AdbpgWriter 针对 ADB PG 类型转换列表:

| DataX 内部类型| ADB PG 数据类型    |
| -------- | -----  |
| Long     |bigint, bigserial, integer, smallint, serial |
| Double   |double precision, float, numeric, real |
| String   |varchar, char, text|
| Date     |date, time, timestamp |
| Boolean  |bool|

## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征
建表语句：
```sql
create table schematest.test_datax (
                                          t1 int,
                                          t2 bigint,
                                          t3 bigserial,
                                          t4 float,
                                          t5 timestamp,
                                          t6 varchar
)distributed by(t1);

```

#### 4.1.2 机器参数

* 执行DataX的机器参数为:
	1. cpu: 24核
	2. mem: 96GB


* ADB PG数据库机器参数为:
	1. 平均core数量:4
	2. primary segment 数量: 4  
	3. 计算组数量:2
### 4.2 测试报告

#### 4.2.1 单表测试报告

| 通道数|  commitSize MB | DataX速度(Rec/s)| DataX流量(M/s) 
|--------|--------| --------|--------|
|1| 10 | 54098 | 15.54 |
|1| 20 | 55000 | 15.80 | 
|4| 10 | 183333 | 52.66 | 
|4| 20 | 173684 | 49.89 |
|8| 10 | 330000 | 94.79 | 
|8| 20 | 300000 | 86.17 | 
|16| 10 | 412500 | 118.48 |
|16| 20 | 366666 | 105.32 | 
|32| 10 | 366666 | 105.32 | 

#### 4.2.2 性能测试小结
1. `channel数对性能影响很大`
2. `通常不建议写入数据库时，通道个数 > 32`
