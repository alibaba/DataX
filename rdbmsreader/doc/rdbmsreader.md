# RDBMSReader 插件文档


___


## 1 快速介绍

RDBMSReader插件实现了从RDBMS读取数据。在底层实现上，RDBMSReader通过JDBC连接远程RDBMS数据库，并执行相应的sql语句将数据从RDBMS库中SELECT出来。目前支持达梦、db2、PPAS、Sybase数据库的读取。RDBMSReader是一个通用的关系数据库读插件，您可以通过注册数据库驱动等方式增加任意多样的关系数据库读支持。


## 2 实现原理

简而言之，RDBMSReader通过JDBC连接器连接到远程的RDBMS数据库，并根据用户配置的信息生成查询SELECT SQL语句并发送到远程RDBMS数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column、Where的信息，RDBMSReader将其拼接为SQL语句发送到RDBMS数据库；对于用户配置querySql信息，RDBMS直接将其发送到RDBMS数据库。


## 3 功能说明

### 3.1 配置样例

* 配置一个从RDBMS数据库同步抽取数据作业:

```
{
    "job": {
        "setting": {
            "speed": {
                "byte": 1048576
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0.02
            }
        },
        "content": [
            {
                "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "xxx",
                        "password": "xxx",
                        "column": [
                            "id",
                            "name"
                        ],
                        "splitPk": "pk",
                        "connection": [
                            {
                                "table": [
                                    "table"
                                ],
                                "jdbcUrl": [
                                    "jdbc:dm://ip:port/database"
                                ]
                            }
                        ],
                        "fetchSize": 1024,
                        "where": "1 = 1"
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

* 配置一个自定义SQL的数据库同步任务到ODPS的作业：

```
{
    "job": {
        "setting": {
            "speed": {
                "byte": 1048576
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0.02
            }
        },
        "content": [
            {
                "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "xxx",
                        "password": "xxx",
                        "column": [
                            "id",
                            "name"
                        ],
                        "splitPk": "pk",
                        "connection": [
                            {
                                "querySql": [
                                    "SELECT * from dual"
                                ],
                                "jdbcUrl": [
                                    "jdbc:dm://ip:port/database"
                                ]
                            }
                        ],
                        "fetchSize": 1024,
                        "where": "1 = 1"
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


### 3.2 参数说明

* **jdbcUrl**

  * 描述：描述的是到对端数据库的JDBC连接信息，jdbcUrl按照RDBMS官方规范，并可以填写连接附件控制信息。请注意不同的数据库jdbc的格式是不同的，DataX会根据具体jdbc的格式选择合适的数据库驱动完成数据读取。
  
	- 达梦 jdbc:dm://ip:port/database
	- db2格式 jdbc:db2://ip:port/database
	- PPAS格式 jdbc:edb://ip:port/database
  
	**rdbmswriter如何增加新的数据库支持:**  
	
	- 进入rdbmsreader对应目录，这里${DATAX_HOME}为DataX主目录，即: ${DATAX_HOME}/plugin/reader/rdbmswriter
	- 在rdbmsreader插件目录下有plugin.json配置文件，在此文件中注册您具体的数据库驱动，具体放在drivers数组中。rdbmsreader插件在任务执行时会动态选择合适的数据库驱动连接数据库。


	```
	{
	    "name": "rdbmsreader",
	    "class": "com.alibaba.datax.plugin.reader.rdbmsreader.RdbmsReader",
	    "description": "useScene: prod. mechanism: Jdbc connection using the database, execute select sql, retrieve data from the ResultSet. warn: The more you know about the database, the less problems you encounter.",
	    "developer": "alibaba",
	    "drivers": [
	        "dm.jdbc.driver.DmDriver",
	        "com.ibm.db2.jcc.DB2Driver",
	        "com.sybase.jdbc3.jdbc.SybDriver",
	        "com.edb.Driver"
	    ]
	}
	```
        
	- 在rdbmsreader插件目录下有libs子目录，您需要将您具体的数据库驱动放到libs目录下。

	
	```
	$tree
	.
	|-- libs
	|   |-- Dm7JdbcDriver16.jar
	|   |-- commons-collections-3.0.jar
	|   |-- commons-io-2.4.jar
	|   |-- commons-lang3-3.3.2.jar
	|   |-- commons-math3-3.1.1.jar
	|   |-- datax-common-0.0.1-SNAPSHOT.jar
	|   |-- datax-service-face-1.0.23-20160120.024328-1.jar
	|   |-- db2jcc4.jar
	|   |-- druid-1.0.15.jar
	|   |-- edb-jdbc16.jar
	|   |-- fastjson-1.1.46.sec01.jar
	|   |-- guava-r05.jar
	|   |-- hamcrest-core-1.3.jar
	|   |-- jconn3-1.0.0-SNAPSHOT.jar
	|   |-- logback-classic-1.0.13.jar
	|   |-- logback-core-1.0.13.jar
	|   |-- plugin-rdbms-util-0.0.1-SNAPSHOT.jar
	|   `-- slf4j-api-1.7.10.jar
	|-- plugin.json
	|-- plugin_job_template.json
	`-- rdbmsreader-0.0.1-SNAPSHOT.jar
	```
  
  
  * 必选：是 <br />
  
  * 默认值：无 <br />
 
* **username**
 
  * 描述：数据源的用户名。 <br />
 
  * 必选：是 <br />
 
  * 默认值：无 <br />

* **password**

  * 描述：数据源指定用户名的密码。 <br />
 
  * 必选：是 <br />
 
  * 默认值：无 <br />
 
* **table**

  * 描述：所选取的需要同步的表名。<br />
 
  * 必选：是 <br />
 
  * 默认值：无 <br />
 
* **column**

  * 描述：所配置的表中需要同步的列名集合，使用JSON的数组描述字段信息。用户使用*代表默认使用所有列配置，例如['*']。  
  
    支持列裁剪，即列可以挑选部分列进行导出。

      支持列换序，即列可以不按照表schema信息进行导出。
      
    支持常量配置，用户需要按照JSON格式:
    ["id", "1", "'bazhen.csy'", "null", "to_char(a + 1)", "2.3" , "true"]
    id为普通列名，1为整形数字常量，'bazhen.csy'为字符串常量，null为空指针，to_char(a + 1)为表达式，2.3为浮点数，true为布尔值。
    
    Column必须显示填写，不允许为空！

  * 必选：是 <br />
 
  * 默认值：无 <br />
 
* **splitPk**

  * 描述：RDBMSReader进行数据抽取时，如果指定splitPk，表示用户希望使用splitPk代表的字段进行数据分片，DataX因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能。 
  
    推荐splitPk用户使用表主键，因为表主键通常情况下比较均匀，因此切分出来的分片也不容易出现数据热点。
    
    目前splitPk仅支持整形数据切分，`不支持浮点、字符串型、日期等其他类型`。如果用户指定其他非支持类型，RDBMSReader将报错！
 
      splitPk如果不填写，将视作用户不对单表进行切分，RDBMSReader使用单通道同步全量数据。

  * 必选：否 <br />
 
  * 默认值：空 <br />

* **where**
 
  * 描述：筛选条件，RDBMSReader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。例如在做测试时，可以将where条件指定为limit 10；在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为gmt_create > $bizdate 。<br />。
  
          where条件可以有效地进行业务增量同步。where条件不配置或者为空，视作全表同步数据。
 
  * 必选：否 <br />
 
  * 默认值：无 <br />

* **querySql**

  * 描述：在有些业务场景下，where这一配置项不足以描述所筛选的条件，用户可以通过该配置型来自定义筛选SQL。当用户配置了这一项之后，DataX系统就会忽略table，column这些配置型，直接使用这个配置项的内容对数据进行筛选，例如需要进行多表join后同步数据，使用select a,b from table_a join table_b on table_a.id = table_b.id <br />
 
   `当用户配置querySql时，RDBMSReader直接忽略table、column、where条件的配置`。
   
  * 必选：否 <br />
 
  * 默认值：无 <br />

* **fetchSize**

  * 描述：该配置项定义了插件和数据库服务器端每次批量数据获取条数，该值决定了DataX和服务器端的网络交互次数，能够较大的提升数据抽取性能。<br />
 
   `注意，该值过大(>2048)可能造成DataX进程OOM。`。
   
  * 必选：否 <br />
 
  * 默认值：1024 <br />


### 3.3 类型转换

目前RDBMSReader支持大部分通用得关系数据库类型如数字、字符等，但也存在部分个别类型没有支持的情况，请注意检查你的类型，根据具体的数据库做选择。
