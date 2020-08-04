# DataX HologresJdbcWriter


---


## 1 快速介绍

HologresJdbcWriter插件实现了写入数据到hologres的功能。在底层实现上，PostgresqlWriter通过JDBC连接Hologres，并执行相应的 insert into ... sql 语句将数据写入Hologres，内部会根据配置攒批提交入库。




## 2 实现原理

HologresJdbcWriter通过 DataX 框架获取 Reader 生成的协议数据，根据你配置生成相应的SQL插入语句


* `insert into...`(当存在 主键/唯一性索引 时必须配置writeMode 可支持update 或 ignore)




## 3 功能说明

### 3.1 配置样例

* 这里使用一份从内存产生到 PostgresqlWriter导入的数据。

```json
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
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
                                "value": "464,98661,32489",       //integer数组
                                "type": "string"
                            },
                            {
                                "value": "8.58967,96.4667,9345.16",  //real数组
                                "type": "string"
                            },

                        ],
                        "sliceRecordCount": 1000
                    }
                },
                "writer": {
                    "name": "hologresjdbcwriter",
                    "parameter": {
                        "username": "xx",
                        "password": "xx",
                        "column": [
                            "name",
                            "id",
                            "date",
                            "done",
                            "integerarr",
                            "realarr"
                        ],
                        "arrayType": ["integer","real"],
                        "arrayDelimiter": ",",
                        "batchSize": 100,
                        "writeMode": "ignore",
                        "primaryKey": ["id"],
                        "preSql": [
                            "delete from test"
                        ],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:postgresql://127.0.0.1:3002/dbname",
                                "table": [
                                    "test"
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


### 3.2 参数说明

* **jdbcUrl**

    * 描述：目的数据库的 JDBC 连接信息 ,jdbcUrl必须包含在connection配置单元中。

      注意：hologres的后端为postgres 所以jdbcUrl必须为jdbc:postgresql://endpoint/dbname 的形式。 


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

* **table**

  * 描述：目的表的表名称。支持写入一个或者多个表。当配置为多张表时，必须确保所有表结构保持一致。

               注意：table 和 jdbcUrl 必须包含在 connection 配置单元中

  * 必选：是 <br />

  * 默认值：无 <br />

* **column**

  * 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。如果要依次写入全部列，使用*表示, 例如: "column": ["*"]

               注意：1、我们强烈不推荐你这样配置，因为当你目的表字段个数、类型等有改动时，你的任务可能运行不正确或者失败
                    2、此处 column 不能配置任何常量值

  * 必选：是 <br />

  * 默认值：否 <br />

* **arrayType**

  * 描述：目的表需要写入数据的数组的元素类型,类型之间用英文逗号分隔。例如: "arrayType": ["integer","real"]。

               注意：对目的表写入的列类型包含数组类型时，必须指定该字段，并将数组元素类型信息按照column字段的顺序依次指定（非数组类型不需指定）
                 

  * 必选：否 <br />

  * 默认值：无 <br />
 
* **arrayDelimiter**

  * 描述：数组元素的分隔符，由于datax不支持数组类型 ，所以写入hologres的数组数据只能对应datax的string类型，元素在string中以arrayDelimiter指定的分隔符分隔。
  可参考3.1配置样例

               注意：对目的表写入的列类型包含数组类型时，必须指定该字段
                 

  * 必选：否 <br />

  * 默认值：无 <br />

* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与hologres的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。<br />

	* 必选：否 <br />

	* 默认值：1 <br />
	
* **writeMode**

	* 描述：向目的表写入的模式，当目的表无主键时应可忽略，或者显式设置为 "insert"。 当目的表存在 主键/唯一性索引 时 必须指定为"update"或"ignore"，"<br />

	* 必选：否 <br />

	* 默认值：insert <br />
	
* **primaryKey**

	* 描述：目的表的 主键名/唯一性索引名， 当目的表存在主键/唯一性索引时必须指定，当存在联合主键时，主键名之间用英文逗号分隔，例如"primaryKey": ["id"，"age"]"<br />

	* 必选：否 <br />

	* 默认值：无 <br />
	    
* **preSql**

  * 描述：写入数据到目的表前，会先执行这里的标准语句。如果 Sql 中有你需要操作到的表名称，请使用 `@table` 表示，这样在实际执行 Sql 语句时，会对变量按照实际表名称进行替换。比如你的任务是要写入到目的端的100个同构分表(表名称为:datax_00,datax01, ... datax_98,datax_99)，并且你希望导入数据前，先对表中数据进行删除操作，那么你可以这样配置：`"preSql":["delete from @table"]`，效果是：在执行到每个表写入数据前，会先执行对应的 delete from 对应表名称 <br />

  * 必选：否 <br />

  * 默认值：无 <br />

* **postSql**

  * 描述：写入数据到目的表后，会执行这里的标准语句。（原理同 preSql ） <br />

  * 必选：否 <br />

  * 默认值：无 <br />


### 3.3 类型转换

目前 PostgresqlWriter支持大部分 PostgreSQL类型，但也存在部分没有支持的情况，请注意检查你的类型。

下面列出 PostgresqlWriter针对 PostgreSQL类型转换列表:

| DataX 内部类型| PostgreSQL 数据类型    |
| -------- | -----  |
| Long     |bigint, integer, smallint |
| Double   |double precision, money, numeric, real |
| String   |varchar, char, text, bit, array|
| Date     |date, time, timestamp |
| Boolean  |bool|
| Bytes    |bytea|

### 3.4 数组类型支持

datax本身不支持数组类型，目前 PostgresqlWriter支持部分 PostgreSQL数组类型，但也存在部分没有支持的情况，请注意检查你的类型。

下面列出 PostgresqlWriter所支持的数组元素类型:

| PostgreSQL 数组元素类型    |
| -----  |
| smallint  |
| integer  |
|bigint |
| real  |
| double  precision |
| numeric |
| text   |
| varchar   |
| boolean  |


