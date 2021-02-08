# DataX HiveReader 插件文档

------------

## 1 快速介绍

HiveReader提供了读取Hive数据仓库数据存储的能力，支持以自定义SQL查询语句以读取数据。在底层实现上，通过hivd-jdbc连接远程Hive数据仓库，并根据用户配置的SQL语句进行查询，将查询结果转化为DataX自定义的数据类型传递给下游Writer处理。


## 2 功能与限制

HiveReader实现了通过自定义SQL语句读取Hive表中全量或部分数据，并将其转为DataX协议供下游Writer读取的功能。

1. **支持以传参形式传入自定义SQL语句，进行部分表数据的读取与同步。**
2. **支持一次性传递多条SQL语句（但需保证查询结果的表结构一致）。**
3. **多条SQL语句会均分至各Task，进行解释查询**
4. **支持Kerberos认证**
5. **目前插件中hive-jdbc版本为1.1.1，兼容在这之上高版本的HiveServer2**



## 3 功能说明


### 3.1 配置样例

```json
{
    "job": {
        "setting": {
            "speed": {
                "channel": 3
            }
        },
        "content": [
            {
                "reader": {
                    "name": "hivereader",
                    "parameter": {
                        "sqls": [
                            "select * from table1 where condition=1",
                            "select * from table2 where condition=2"
                        ],
                        "jdbcUrl": "jdbc:hive2://ip:port",
                        "user": "hive",
                        "password": ""
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

### 3.2 参数说明（各个配置项值前后不允许有空格）

- **sqls**
  - 描述：用户自定义SQL查询语句，用于查询表中部分数据进行同步。数组类型，支持传递多条SQL语句<br />
  - 必选：是 <br />
  - 默认值：无 <br />
- **jdbcUrl**
  - 描述：用于连接HiveServer2的jdbc连接url<br />
  - 必选：是 <br />
  - 默认值：无 <br />
- **user**
  - 描述：用于连接HiveServer2的用户名<br />
  - 必选：是 <br />
  - 默认值：无 <br />
- **password**
  - 描述：用于连接HiveServer2的密码<br />
  - 必选：是 <br />
  - 默认值：无 <br />
* **haveKerberos**

  * 描述：是否有Kerberos认证，默认false<br />

  	 例如如果用户配置true，则配置项kerberosKeytabFilePath，kerberosPrincipal为必填。
  	
   	* 必选：haveKerberos 为true必选 <br />

    * 默认值：false <br />

* **kerberosKeytabFilePath**

  * 描述：Kerberos认证 keytab文件路径，绝对路径<br />

   	* 必选：否 <br />

    * 默认值：无 <br />

* **kerberosPrincipal**

  * 描述：Kerberos认证Principal名，如xxxx/hadoopclient@xxx.xxx <br />

   	* 必选：haveKerberos 为true必选 <br />

    * 默认值：无 <br />

### 3.3 类型转换

HiveReader提供了类型转换的建议表如下：

| DataX 内部类型| Hive表 数据类型    |
| -------- | -----  |
| Long     |TINYINT,SMALLINT,INT,BIGINT|
| Double   |FLOAT,DOUBLE|
| String   |String,CHAR,VARCHAR,STRUCT,MAP,ARRAY,UNION,BINARY|
| Boolean  |BOOLEAN|
| Date     |Date,TIMESTAMP|

特别提醒：

* Hive支持的数据类型TIMESTAMP可以精确到纳秒级别，所以TIMESTAMP存放的数据类似于"2015-08-21 22:40:47.397898389"，转化为DataX的Date会导致纳秒部分丢失。


## 4 性能报告



## 5 约束限制

### 	5.1 SQL安全性

HiveReader提供querySql语句交给用户自己实现SELECT抽取语句，HiveReader本身对querySql不做任何安全性校验。这块交由DataX用户方自己保证。

## 6 FAQ

### 	6.1 hive与hive-jdbc版本匹配问题

若Hive为CDH版本，在使用HiveReader中的部分表数据同步功能时，其hive-jdbc也应与Hive保持一致同为CDH版本。

