# DataX TDengineWriter

[简体中文](./tdenginewriter-CN.md) | English

## 1 Quick Introduction

TDengineWriter Plugin writes data to [TDengine](https://www.taosdata.com/en/). It can be used to offline synchronize data from other databases to TDengine.

## 2 Implementation

TDengineWriter get records from DataX Framework that are generated from reader side. It has two whiting strategies:

1. For data from OpenTSDBReader which is in json format, to leverage the new feature of TDengine Server that support writing json data directly called [schemaless writing](https://www.taosdata.com/cn/documentation/insert#schemaless), we use JNI to call functions in `taos.lib` or `taos.dll`.(Since the feature was not included in taos-jdbcdrive until version 2.0.36).
2. For other data sources, we use [taos-jdbcdriver](https://www.taosdata.com/cn/documentation/connector/java) to write data. If the target table is not exists beforehand, then it will be created automatically according to your configuration.

## 3 Features Introduction
### 3.1 From OpenTSDB to TDengine
#### 3.1.1 Sample Setting

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "opentsdbreader",
          "parameter": {
            "endpoint": "http://192.168.1.180:4242",
            "column": [
              "weather_temperature"
            ],
            "beginDateTime": "2021-01-01 00:00:00",
            "endDateTime": "2021-01-01 01:00:00"
          }
        },
        "writer": {
          "name": "tdenginewriter",
          "parameter": {
            "host": "192.168.1.180",
            "port": 6030,
            "dbName": "test",
            "username": "root",
            "password": "taosdata"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 1
      }
    }
  }
}
```

#### 3.1.2 Configuration

| Parameter | Description                    | Required | Default  |
| --------- | ------------------------------ | -------- | -------- |
| host      | host of TDengine               | Yes      |          |
| port      | port of TDengine               | Yes      |          |
| username      | use name of TDengine           | No       | root     |
| password  | password of TDengine           | No       | taosdata |
| dbName    | name of target database        | No       |          |
| batchSize | batch size of insert operation | No       | 1        |


#### 3.1.3 Type Convert

| OpenTSDB Type    | DataX Type | TDengine Type |
| ---------------- | ---------- | ------------- |
| timestamp        | Date       | timestamp     |
| Integer（value） | Double     | double        |
| Float（value）   | Double     | double        |
| String（value）  | String     | binary        |
| Integer（tag）   | String     | binary        |
| Float（tag）     | String     | binary        |
| String（tag）    | String     | binary        |

### 3.2 From MongoDB to TDengine

#### 3.2.1 Sample Setting
```json
{
    "job": {
        "setting": {
            "speed": {
                "channel": 2
            }
        },
        "content": [
            {
                "reader": {
                    "name": "mongodbreader",
                    "parameter": {
                        "address": [
                            "127.0.0.1:27017"
                        ],
                        "userName": "user",
                        "mechanism": "SCRAM-SHA-1",
                        "userPassword": "password",
                        "authDb": "admin",
                        "dbName": "test",
                        "collectionName": "stock",
                        "column": [
                            {
                              "name": "stockID",
                              "type": "string"  
                            },
                            {
                                "name": "tradeTime",
                                "type": "date"
                            },
                            {
                                "name": "lastPrice",
                                "type": "double"
                            },
                            {
                              "name": "askPrice1",
                              "type": "double"
                            },
                            {
                              "name": "bidPrice1",
                              "type": "double"
                            },
                            {
                                "name": "volume",
                                "type": "int"
                            }
                        ]
                    }
                },
                "writer": {
                    "name": "tdenginewriter",
                    "parameter": {
                        "host": "localhost",
                        "port": 6030,
                        "dbName": "test",
                        "username": "root",
                        "password": "taosdata",
                        "stable": "stock",
                        "tagColumn": {
                          "industry": "energy",
                          "stockID": 0
                        },
                        "fieldColumn": {
                          "lastPrice": 2,
                          "askPrice1": 3,
                          "bidPrice1": 4,
                          "volume": 5
                        },
                        "timestampColumn": {
                          "tradeTime": 1
                        }
                    }
                }
            }
        ]
    }
}
```

**Note：the writer part of this setting can also apply to other data source except for OpenTSDB **


#### 3.2.2 Configuration

| Parameter       | Description                                                     | Required                 | Default  | Remark              |
| --------------- | --------------------------------------------------------------- | ------------------------ | -------- | ------------------- |
| host            | host ofTDengine                                                 | Yes                      |          |
| port            | port of TDengine                                                | Yes                      |          |
| username            | username of TDengine                                           | No                       | root     |
| password        | password of TDengine                                            | No                       | taosdata |
| dbName          | name of target database                                         | Yes                      |          |
| batchSize       | batch size of insert operation                                  | No                       | 1000     |
| stable          | name of target super table                                      | Yes(except for OpenTSDB) |          |
| tagColumn       | name and position of tag columns in the record from reader, format:{tagName1: tagInd1, tagName2: tagInd2}      | No                       |          | index starts with 0 |
| fieldColumn     | name and position of data columns in the record from reader, format: {fdName1: fdInd1, fdName2: fdInd2}     | No                       |          |                     |
| timestampColumn | name and position of timestamp column in the record from reader | No                       |          |                     |

**Note**: You see that the value of tagColumn "industry" is a fixed string, this ia a good feature of this plugin. Think about this scenario: you have many tables with the structure and one table corresponds to one device. You want to use the device number as a tag in the target super table, then this feature is designed for you.

#### 3.2.3 Auto table creating
##### 3.2.3.1 Rules

If all of `tagColumn`, `fieldColumn` and `timestampColumn` are offered in writer configuration, then target super table will be created automatically.
The type of tag columns will always be `NCHAR(64)`. The sample setting above will produce following sql:

```sql
CREATE STABLE IF NOT EXISTS market_snapshot (
  tadetime TIMESTAMP,
  lastprice DOUBLE,
  askprice1 DOUBLE,
  bidprice1 DOUBLE,
  volume INT
)
TAGS(
  industry NCHAR(64),
  stockID NCHAR(64)
);
```

##### 3.2.3.2 Sub-table Creating Rules

The structure of sub-tables are the same with structure of super table. The names of sub-tables are generated by rules below:
1. combine value of tags like this:`tag_value1!tag_value2!tag_value3`.
2. compute md5 hash hex of  above string, named `md5val`
3. use "t_md5val" as sub-table name, in which "t" is fixed prefix.

#### 3.2.4 Use Pre-created Table

If you have created super table firstly, then all of tagColumn, fieldColumn and timestampColumn can be omitted. The writer plugin will get table schema by executing `describe stableName`.
The order of columns of records received by this plugin must be the same as the order of columns returned by `describe stableName`. For example, if you have super table as below:
```
             Field              |         Type         |   Length    |   Note   |
=================================================================================
 ts                             | TIMESTAMP            |           8 |          |
 current                        | DOUBLE               |           8 |          |
 location                      | BINARY                |           10 | TAG      |
```
Then the first columns received by this writer plugin must represent timestamp, the second column must represent current with type double, the third column must represent location with internal type string.

#### 3.2.5 Remarks

1. Config keys --tagColumn, fieldColumn and timestampColumn, must be presented or omitted at the same time.  
2. If above three config keys exist and the target table also exists, then the order of columns defined by the config file and the existed table must be the same.

#### 3.2.6 Type Convert

|DataX Type | TDengine Type |
|-------------- | ----------------- |
|Long           | BIGINT            |
|Double         | DOUBLE            |
|String         | NCHAR(64)         |
|Date           | TIMESTAMP         |
|Boolean        | BOOL              |
|Bytes          | BINARY(64)        |

### 3.3 From Relational Database to TDengine

Take MySQl as example.

#### 3.3.1 Table Structure in MySQL
```sql
CREATE TABLE IF NOT EXISTS weather(
    station varchar(100),
    latitude DOUBLE,
    longtitude DOUBLE,
    `date` DATE,
    TMAX int,
    TMIN int
)
```

#### 3.3.2 Sample Setting

```json 
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "username": "root",
            "password": "passw0rd",
            "column": [
              "*"
            ],
            "splitPk": "station",
            "connection": [
              {
                "table": [
                  "weather"
                ],
                "jdbcUrl": [
                  "jdbc:mysql://127.0.0.1:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8"
                ]
              }
            ]
          }
        },
        "writer": {
          "name": "tdenginewriter",
          "parameter": {
            "host": "127.0.0.1",
            "port": 6030,
            "dbName": "test",
            "username": "root",
            "password": "taosdata",
            "batchSize": 1000,
            "stable": "weather",
            "tagColumn": {
              "station": 0
            },
            "fieldColumn": {
              "latitude": 1,
              "longtitude": 2,
              "tmax": 4,
              "tmin": 5
            },
            "timestampColumn":{
              "date": 3
            }
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 1
      }
    }
  }
}
```


## 4 Performance Test

## 5 Restriction

1. NCHAR type has fixed length 64 when auto creating stable.
2. Rows have null tag values will be dropped.

## FAQ

### How to filter on source table？

It depends on reader plugin. For different reader plugins, the way may be different.

### How to import multiple source tables at once？

It depends on reader plugin. If the reader plugin supports reading multiple tables at once, then there is no problem. 

### How many sub-tables will be produced?

The number of sub-tables is determined by tagColumns, equals to the number of different combinations of tag values. 

### Do columns in source table and columns in target table must be in the same order?

No. TDengine require the first column has timestamp type，which is followed by data columns, followed by tag columns. The writer plugin will create super table in this column order, regardless of origin column orders.

### How dose the plugin infer the data type of incoming data?

By the first batch of records it received.

###  Why can't I insert data of 10 years ago? Do this will get error: `TDengine ERROR (2350): failed to execute batch bind`.

Because the database you created only keep 10 years data by default, you can create table like this: `CREATE DATABASE power KEEP 36500;`, in order to enlarge the time period to 100 years.


### What should I do if some dependencies of a plugin can't be found?

I this plugin is not necessary for you, just remove it from pom.xml under project's root directory.
