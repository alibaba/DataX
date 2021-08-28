## 快速介绍

```sahistoryreader```是用于将hive中的数据通过jdbc方式导出的插件，可以使用两种方式之一，第一种是使用row_number()函数方式通过分页查询数据，第二种是使用时间字段条件过滤查询数据。

## **实现原理**

```sahistoryreader```是通过hive的row_number()函数方式实现分页，或者时间条件过滤数据。

## 配置说明

### 使用row_number()函数方式

以下配置仅row_number()方式全部配置参数

```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "sahistoryreader",
                    "parameter": {
                        "column": [
                            "name","age","id","update_date","date_str"
                        ],
                        "hiveVersion": "1.0.0",
                        "pageSize": 15,
                        "password": "",
                        "receivePageSize": 10,
                        "sa": {
                            "hiveUrl": "jdbc:hive2://10.120.232.3:10000/test",
                            "table": "apple"
                        },
                        "useRowNumber": true,
                        "username": "",
                        "where": "age > 18"
                    }
                },
                "writer": {
                    "name": "xxx",
                    "parameter": {
                       ...
                    }
                }
            }
        ],
        "setting": {
            "speed": {
                "channel": "1"
            }
        }
    }
}
```

### 使用时间字段条件过滤方式

以下配置仅时间字段条件过滤方式全部配置参数

```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "sahistoryreader",
                    "parameter": {
                        "column": [
                            "name","age","id","update_date","date_str"
                        ],
                        "hiveVersion": "1.0.0",
                        "datePattern": "yyyy-MM-dd",
                        "endTime": "2021-06-24",
                        "password": "",
                        "sa": {
                            "hiveUrl": "jdbc:hive2://10.120.232.3:10000/test",
                            "table": "apple"
                        },
                        "startTime": "2021-06-14",
                        "taskNum": 5,
                        "timeFieldName": "update_date",
                        "timeInterval": 1000,
                        “maxQueryNum”: 50000,
                        "useRowNumber": false,
                        "username": ""
                    }
                },
                "writer": {
                    "name": "xxxx",
                    "parameter": {
                        ...
                    }
                }
            }
        ],
        "setting": {
            "speed": {
                "channel": "1"
            }
        }
    }
}
```

## **参数说明**

### ``read``

​		```hiveVersion```：需要使用的hive版本，该属性取值为读插件的hivelib目录下的某一文件夹名称，在该文件夹下放入配置的hive版本所需要的依赖（不要存在子文件夹）,该值必须配置。例如：在该读插件的hivelib有文件夹1.0.0，则该参数值为1.0.0，在1.0.0目录下需要放置以下两个主依赖（hive-jdbc、hadoop-common）下的所有依赖。

```xml
<groupId>org.apache.hive</groupId>
<artifactId>hive-jdbc</artifactId>

<groupId>org.apache.hadoop</groupId>
<artifactId>hadoop-common</artifactId>
```

​		`column`：hive表中需要查询的字段名列表

​		`pageSize`：使用row_number()方式时，分页的大小，默认值10000。

​		`password`：连接hive的密码。

​		`receivePageSize`：使用row_number()方式时，每个task负责的页数，默认值5。

​		`sa.hiveUrl`：hive连接的url。

​		`sa.table`：hive要查询的表。

​		`useRowNumber`：是否使用row_number()方式，false或为空时，使用时间字段条件过滤方式。

​		`username`：连接hive的用户名。

​		`where`：使用任意方式时的查询条件。

​		`datePattern`：使用时间字段条件过滤方式时，时间格式。

​		`endTime`：使用时间字段条件过滤方式时，条件结束时间（不包含）。

​		`startTime`：使用时间字段条件过滤方式时，条件开始时间。

​		`taskNum`：使用时间字段条件过滤方式时，task数量，默认值为dataX框架提供的值。

​		`timeFieldName`：使用时间字段条件过滤方式时，使用的时间条件字段名。

​		`timeInterval`：使用时间字段条件过滤方式时，时间段通过```taskNum```分片后，如果数据量还是过大时可指定每次查多久的，默认值为查询一天的量，单位毫秒。

​		```maxQueryNum```：使用时间字段条件过滤方式时，当```timeInterval```时间段内超过该值时，将继续拆分直到拆分的时间段内的数量小于等于该值，时间段采用二分对半拆分，默认值为50000。

​		```timeFieldCount```：使用时间字段条件过滤方式时，是否先统计数量再查询，默认值true，表示优先使用先统计数量再拉取数据，当数据量大时可防止OOM,若为false,则直接拉取数据，不在进行统计，可能导致OOM。

## **类型转换**

###  读插件

|                             java                             |    dataX     |    dataX实际类型     |
| :----------------------------------------------------------: | :----------: | :------------------: |
|                             null                             | StringColumn |   java.lang.String   |
|                       java.lang.String                       | StringColumn |   java.lang.String   |
|                  boolean/java.long.Boolean                   |  BoolColumn  |  java.lang.Boolean   |
| byte/java.long.Byte/short/java.long.Short/int/java.long.Integer/long/java.long.Long |  LongColumn  | java.math.BigInteger |
|        float/java.long.Float/double/java.long.Double         | DoubleColumn |   java.lang.String   |
|                        java.util.Date                        |  DateColumn  |    java.util.Date    |
|                     java.time.LocalDate                      |  DateColumn  |    java.util.Date    |
|                   java.time.LocalDateTime                    |  DateColumn  |    java.util.Date    |
|                        java.sql.Date                         |  DateColumn  |    java.util.Date    |
|                      java.sql.Timestamp                      |  DateColumn  |    java.util.Date    |

