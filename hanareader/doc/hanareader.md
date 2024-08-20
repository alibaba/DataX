# HanaReader 插件文档


___


## 1 快速介绍

HanaReader插件实现了从SAP HANA读取数据。在底层实现上，HanaReader通过JDBC连接远程SAP HANA数据库，并执行相应的sql语句将数据从SAP HANA库中SELECT出来。

## 2 实现原理

简而言之，HanaReader通过JDBC连接器连接到远程的SAP HANA数据库，并根据用户配置的信息生成查询SELECT SQL语句并发送到远程SAP HANA数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column、Where的信息，HanaReader将其拼接为SQL语句发送到SAP HANA数据库；对于用户配置querySql信息，HanaReader直接将其发送到SAP HANA数据库。


## 3 功能说明

### 3.1 配置样例

* 配置一个从SAP HANA数据库同步抽取数据到本地的作业:

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "hanareader",
          "parameter": {
            "username": "SYSTEM",
            "password": "Xing1234",
            "connection": [
              {
                "querySql": [
                  "select ID,MODULE,NAME,TYPE,TIME from T_OPERATOR_LOG"
                ],
                "jdbcUrl": [
                  "jdbc:sap://192.168.16.20:39013?currentschema=SYSTEM&reconnect=true"
                ]
              }
            ]
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print": true,
            "encoding": "UTF-8"
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

      jdbcUrl按照SAP HANA官方规范，并可以填写连接附件控制信息。具体请参看SAP HANA官方文档或者咨询对应 DBA。

    * 必选：是 <br />

    * 默认值：无 <br />

* **username**

    * 描述：数据源的用户名 <br />

    * 必选：是 <br />

    * 默认值：无 <br />

* **password**

    * 描述：数据源指定用户名的密码 <br />

    * 必选：是 <br />

    * 默认值：无 <br />

* **table**

    * 描述：所选取的需要同步的表。使用JSON的数组描述，因此支持多张表同时抽取。当配置为多张表时，用户自己需保证多张表是同一schema结构。注意，table必须包含在connection配置单元中。<br />

    * 必选：是 <br />

    * 默认值：无 <br />

* **column**

    * 描述：所配置的表中需要同步的列名集合，使用JSON的数组描述字段信息。用户使用\*代表默认使用所有列配置，例如['\*']。

      **column必须用户显示指定同步的列集合，不允许为空！**

    * 必选：是 <br />

    * 默认值：无 <br />

* **splitPk**

    * 描述：HanaReader进行数据抽取时，如果指定splitPk，表示用户希望使用splitPk代表的字段进行数据分片，DataX因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能。

      推荐splitPk用户使用表主键，因为表主键通常情况下比较均匀，因此切分出来的分片也不容易出现数据热点。

      目前splitPk仅支持整形数据切分，`不支持浮点、字符串型、日期等其他类型`。如果用户指定其他非支持类型，HanaReader将报错！

      splitPk设置为空，底层将视作用户不允许对单表进行切分，因此使用单通道进行抽取。

    * 必选：否 <br />

    * 默认值：空 <br />

* **where**

    * 描述：筛选条件，HanaReader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。<br />

          where条件可以有效地进行业务增量同步。		where条件不配置或者为空，视作全表同步数据。

    * 必选：否 <br />

    * 默认值：无 <br />

* **querySql**

    * 描述：在有些业务场景下，where这一配置项不足以描述所筛选的条件，用户可以通过该配置型来自定义筛选SQL。当用户配置了这一项之后，DataX系统就会忽略table，column这些配置型，直接使用这个配置项的内容对数据进行筛选 <br />

  `当用户配置querySql时，HanaReader直接忽略table、column、where条件的配置`。

    * 必选：否 <br />

    * 默认值：无 <br />

* **fetchSize**

    * 描述：该配置项定义了插件和数据库服务器端每次批量数据获取条数，该值决定了DataX和服务器端的网络交互次数，能够较大的提升数据抽取性能。<br />

    * 必选：否 <br />

    * 默认值：1024 <br />

### 3.3 类型转换

目前 HanaReader支持大部分 SAP HANA类型，但也存在部分没有支持的情况，请注意检查你的类型。

下面列出 HanaReader针对 SAP HANA类型转换列表:

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
