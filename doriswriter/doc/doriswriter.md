# DorisWriter 插件文档

## 1 快速介绍
DorisWriter支持将大批量数据写入Doris中。

## 2 实现原理
DorisWriter 通过Doris原生支持Stream load方式导入数据， DorisWriter会将`reader`读取的数据进行缓存在内存中，拼接成Json文本，然后批量导入至Doris。

## 3 功能说明

### 3.1 配置样例

这里是一份从Stream读取数据后导入至Doris的配置文件。

```
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0
            }
        },
        "content": [
            {
                "reader": {
                    "name": "streamreader",
                    "parameter": {
                        "column": [
                            {
                                "value": "皮蛋1",
                                "type": "string"
                            },
                            {
                                "value": "皮蛋2",
                                "type": "string"
                            },
                            {
                                "value": "111",
                                "type": "long"
                            },
                            {
                                "value": "222",
                                "type": "long"
                            }
                        ],
                        "sliceRecordCount": 100
                    }
                },
                "writer": {
                    "name": "doriswriter",
                    "parameter": {
                        "username": "hzk",
                        "password": "123456",
                        "database": "test",
                        "table": "datax_test",
                        "column": [
                            "k1",
                            "k2",
                            "v1",
                            "v2"
                        ],
                        "preSql": [],
                        "postSql": [],
                        "jdbcUrl": "jdbc:mysql://10.93.6.247:9030/",
                        "loadUrl": [
                            "10.93.6.167:8041"
                        ],
                        "loadProps": {


                        }
                    }
                }
            }
        ]
    }
}

```



### 3.2 参数说明

* **username**

  - 描述：访问Doris数据库的用户名
  - 必选：是
  - 默认值：无

* **password**

  - 描述：访问Doris数据库的密码
  - 必选：是
  - 默认值：无

* **database**

  - 描述：访问Doris表的数据库名称。
  - 必选：是
  - 默认值：无

* **table**

  - 描述：访问Doris表的表名称。
  - 必选：是
  - 默认值：无

* **loadUrl**

  - 描述：Doris BE的地址用于Stream load，可以为多个BE地址，形如`BE_ip:Be_webserver_port`。
  - 必选：是
  - 默认值：无

* **column**

  - 描述：目的表**需要写入数据**的字段，字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。
  - 必选：是
  - 默认值：否

* **preSql**

  - 描述：写入数据到目的表前，会先执行这里的标准语句。
  - 必选：否
  - 默认值：无

* **postSql**

  - 描述：写入数据到目的表后，会执行这里的标准语句。
  - 必选：否
  - 默认值：无

* **jdbcUrl**

  - 描述：目的数据库的 JDBC 连接信息，用于执行`preSql`及`postSql`。
  - 必选：否
  - 默认值：无

* **loadProps**

  - 描述：StreamLoad 的请求参数，详情参照StreamLoad介绍页面。
  - 必选：否
  - 默认值：无

    

