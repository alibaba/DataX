# DataX StarRocksWriter


---


## 1 快速介绍

StarRocksWriter 插件实现了写入数据到 StarRocks 主库的目的表的功能。在底层实现上， StarRocksWriter 通过Streamload以csv格式导入数据至StarRocks。


## 2 实现原理

 StarRocksWriter 通过Streamload以csv格式导入数据至StarRocks， 内部将`reader`读取的数据进行缓存后批量导入至StarRocks，以提高写入性能。


## 3 功能说明

### 3.1 配置样例

* 这里使用一份从内存Mysql读取数据后导入至StarRocks。

```json
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
                    "name": "mysqlreader",
                    "parameter": {
                        "username": "xxxx",
                        "password": "xxxx",
                        "column": [ "k1", "k2", "v1", "v2" ],
                        "connection": [
                            {
                                "table": [ "table1", "table2" ],
                                "jdbcUrl": [
                                     "jdbc:mysql://127.0.0.1:3306/datax_test1"
                                ]
                            },
                            {
                                "table": [ "table3", "table4" ],
                                "jdbcUrl": [
                                     "jdbc:mysql://127.0.0.1:3306/datax_test2"
                                ]
                            }
                        ]
                    }
                },
               "writer": {
                    "name": "starrockswriter",
                    "parameter": {
                        "username": "xxxx",
                        "password": "xxxx",
                        "column": ["k1", "k2", "v1", "v2"],
                        "preSql": [],
                        "postSql": [], 
                        "connection": [
                            {
                                "table": ["xxx"],
                                "jdbcUrl": "jdbc:mysql://172.28.17.100:9030/",
                                "selectedDatabase": "xxxx"
                            }
                        ],
                        "loadUrl": ["172.28.17.100:8030", "172.28.17.100:8030"],
                        "loadProps": {}
                    }
                }
            }
        ]
    }
}

```


### 3.2 参数说明

* **username**

	* 描述：StarRocks数据库的用户名 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **password**

	* 描述：StarRocks数据库的密码 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **selectedDatabase**

	* 描述：StarRocks表的数据库名称。

	* 必选：是 <br />

	* 默认值：无 <br />

* **table**

	* 描述：StarRocks表的表名称。

	* 必选：是 <br />

	* 默认值：无 <br />

* **loadUrl**

	* 描述：StarRocks FE的地址用于Streamload，可以为多个fe地址，`fe_ip:fe_http_port`。

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。

			**column配置项必须指定，不能留空！**

               注意：我们强烈不推荐你这样配置，因为当你目的表字段个数、类型等有改动时，你的任务可能运行不正确或者失败

	* 必选：是 <br />

	* 默认值：否 <br />

* **preSql**

	* 描述：写入数据到目的表前，会先执行这里的标准语句。 <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **postSql**

	* 描述：写入数据到目的表后，会执行这里的标准语句。 <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **jdbcUrl**

	* 描述：目的数据库的 JDBC 连接信息，用于执行`preSql`及`postSql`。 <br />

 	* 必选：否 <br />

	* 默认值：无 <br />

* **maxBatchRows**

	* 描述：单次StreamLoad导入的最大行数 <br />

 	* 必选：否 <br />

	* 默认值：500000 (50W) <br />

* **maxBatchSize**

	* 描述：单次StreamLoad导入的最大字节数。 <br />

 	* 必选：否 <br />

	* 默认值：104857600 (100M)

* **flushInterval**

	* 描述：上一次StreamLoad结束至下一次开始的时间间隔（单位：ms）。 <br />

 	* 必选：否 <br />

	* 默认值：300000 (ms)

* **loadProps**

	* 描述：StreamLoad 的请求参数，详情参照StreamLoad介绍页面。 <br />

 	* 必选：否 <br />

	* 默认值：无 <br />


### 3.3 类型转换

默认传入的数据均会被转为字符串，并以`\t`作为列分隔符，`\n`作为行分隔符，组成`csv`文件进行StreamLoad导入操作。
如需更改列分隔符， 则正确配置 `loadProps` 即可：
```json
"loadProps": {
    "column_separator": "\\x01",
    "row_delimiter": "\\x02"
}
```

如需更改导入格式为`json`， 则正确配置 `loadProps` 即可：
```json
"loadProps": {
    "format": "json",
    "strip_outer_array": true
}
```

## 4 性能报告


## 5 约束限制


## FAQ
