# DataX HanaWriter


---


## 1 快速介绍

HanaWriter插件实现了写入数据到 SAP HANA 数据库目的表的功能。在底层实现上，HanaWriter通过JDBC连接远程 SAP HANA 数据库，并执行相应的 insert into ... sql 语句将数据写入 SAP HANA，内部会分批次提交入库。

HanaWriter面向ETL开发工程师，他们使用HanaWriter从数仓导入数据到SAP HANA。同时 HanaWriter亦可以作为数据迁移工具为DBA等用户提供服务。


## 2 实现原理

HanaWriter通过 DataX 框架获取 Reader 生成的协议数据，根据你配置生成相应的SQL插入语句


* `insert into...`(当主键/唯一性索引冲突时会写不进去冲突的行)

<br />

    注意：
    1. 目的表所在数据库必须是主库才能写入数据；整个任务至少需具备 insert into...的权限，是否需要其他权限，取决于你任务配置中在 preSql 和 postSql 中指定的语句。
    2. HanaWriter和MysqlWriter不同，不支持配置writeMode参数。


## 3 功能说明

### 3.1 配置样例

* 这里使用一份从mysql到Hana导入的数据。
```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "username": "root",
            "password": "RoOt#2024",
            "connection": [
              {
                "querySql": [
                  "select id,module,name,type,time from test.t_operator_log"
                ],
                "jdbcUrl": [
                  "jdbc:mysql://192.168.16.20:3306/test"
                ]
              }
            ]
          }
        },
        "writer": {
          "name": "hanawriter",
          "parameter": {
            "username": "SYSTEM",
            "password": "Xing1234",
            "column": [
              "ID",
              "MODULE",
              "NAME",
              "TYPE",
              "TIME"
            ],
            "connection": [
              {
                "jdbcUrl": "jdbc:sap://192.168.16.20:39013?currentschema=SYSTEM&reconnect=true",
                "table": [
                  "T_OPERATOR_LOG"
                ]
              }
            ]
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



### 3.2 参数说明

* **jdbcUrl**

    * 描述：目的数据库的 JDBC 连接信息 ,jdbcUrl必须包含在connection配置单元中。

      注意：1、在一个数据库上只能配置一个值。
      2、jdbcUrl按照SAP HANA官方规范，并可以填写连接附加参数信息。具体请参看SAP HANA官方文档或者咨询对应 DBA。


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

    * 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。如果要依次写入全部列，使用\*表示, 例如: "column": ["\*"]

    * 必选：是 <br />

    * 默认值：否 <br />

* **preSql**

    * 描述：写入数据到目的表前，会先执行这里的标准语句。 <br />

    * 必选：否 <br />

    * 默认值：无 <br />

* **postSql**

    * 描述：写入数据到目的表后，会执行这里的标准语句。（原理同 preSql ） <br />

    * 必选：否 <br />

    * 默认值：无 <br />

* **batchSize**

    * 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与SAP HANA的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。<br />

    * 必选：否 <br />

    * 默认值：1024 <br />
    * 

### 3.3 类型转换

目前 HanaWriter支持大部分 SAP HANA类型，但也存在部分没有支持的情况，请注意检查你的类型。

下面列出 HanaWriter针对 SAP HANA类型转换列表:

| DataX 内部类型| SAP HANA 数据类型                     |
| -------- |-----------------------------------|
| Long     | bigint, integer, smallint, serial |
| Double   | double, numeric, real             |
| String   | varchar, char, text               |
| Date     | date, time, timestamp             |
| Boolean  | boolean                           |
| Bytes    | bytea                             |

## 4 性能报告

## FAQ

***
