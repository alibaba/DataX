## 快速介绍

```sacustomizereader```是用于自定义拉取数据的插件，无须关注DataX的处理逻辑，关注自身拉取数据的数据即可。

## **实现原理**

```sacustomizereader```是通过引入``plugin-sa-history-datax-reader-common-plugin``模块，使用者只需关注拉取数据的逻辑。

## 配置说明

```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "sacustomizereader",
                    "parameter": {
                        "column": [
                            "name","age","id","update_date","date_str"
                        ],
                        "plugin": {
                            "name": "MysqlReader",
                            "className": "com.alibaba.datax.MysqlReader",
                            "param": {
                                "mysqlUrl":"jdbc:mysql://localhost:3306/test"
                            }
                        }
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

## **参数说明**

### ``read``

​		`column`：自定义拉取数据的列，注意，该配置中的顺序与发送到写插件的顺序相同。

​		```plugin```：神策写插件的插件列表数组，开发规范见**神策读插件插件规范**。

​		```plugin.name```：插件的名称，该名称为在```sacustomizereader```插件下的``plugin``文件夹下的某一子文件夹名。

​		```plugin.className```：插件的全限定名。

​		```plugin.param```：插件所需要的参数，具体参数根据插件不同而不同。



## **神策读插件插件规范**

- 引入reader common依赖

  ```xml
  <dependency>
      <groupId>com.alibaba</groupId>
      <artifactId>plugin-sa-history-datax-reader-common-plugin</artifactId>
      <version>1.0-SNAPSHOT</version>
  </dependency>
  ```

- 编写代码

  继承``com.alibaba.datax.reader.common.CommonReader``类，重写instance方法（配置文件中parameter下的所有参数以及``plugin.param``的配置项会被传递到该方法中），以及定义内部类继承``com.alibaba.datax.reader.common.CommonReader``的内部类``CommonReader.SAReaderPlugin``，实现``startReadTask``方法，该方法即为自定义拉取数据的入口，该方法中的```columnNameList```参数为配置项中```column```的值。在该方法中调用父类的```buildRecord```方法，构建``DataX``中的一行数据，然后调用``recordSender.sendToWriter(record);``即可将数据发送到写插件中。

- 部署插件

  将插件连同依赖一起打包生成jar包，在datax的```sacustomizereader```插件下新建plugin文件夹，然后再新建一个放置该插件的文件夹，命名无要求，配置文件中```plugin.name```参数为该文件夹名，最后将生成的jar包放置到该文件夹下。

  ***实现原理***

  神策写插件会实例化该类，并调用instance方法获取到``CommonReader.SAReaderPlugin``插件实例，经过生命周期函数后将调用``SAReaderPlugin``的```startReadTask```方法。

  **默认的类型转换**

  |                             java                             |    dataX     |
  | :----------------------------------------------------------: | :----------: |
  |                             null                             | StringColumn |
  |                       java.lang.String                       | StringColumn |
  |                  boolean/java.long.Boolean                   |  BoolColumn  |
  | byte/java.long.Byte/short/java.long.Short/int/java.long.Integer/long/java.long.Long |  LongColumn  |
  |        float/java.long.Float/double/java.long.Double         | DoubleColumn |
  |                        java.util.Date                        |  DateColumn  |
  |                     java.time.LocalDate                      |  DateColumn  |
  |                   java.time.LocalDateTime                    |  DateColumn  |
  |                        java.sql.Date                         |  DateColumn  |
  |                      java.sql.Timestamp                      |  DateColumn  |
  |                   java.lang.Byte[]/byte[]                    | BytesColumn  |

其他未处理的类型，可通过重写```addColumn```方法处理。