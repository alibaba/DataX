## 快速介绍

```sahistorywriter```是用户将数据导入到神策分析的插件。

## **实现原理**

```sahistorywriter	```是通过神策分析java SDK将数据生成符合神策分析的数据格式，也支持通过神策分析数据接口接收数据（数据不落磁盘）。

## 配置说明

```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "xxxx",
                    "parameter": {
                       ...
                    }
                },
                "writer": {
                    "name": "sahistorywriter",
                    "parameter": {
                        "item": {
                            "itemIdColumn": "id1",
                            "itemType": "couse",
                            "typeIsColumn": false
                        },
                        "column": [
                            {
                                "index":0,
                                "name": "name1",
                                "ifNullGiveUp" : true
                            },
                            {
                                "index":1,
                                "name": "age1",
                                "dataConverters":[
                                    {
                                        "type": "Number2Str"
                                    }
                                ]
                            },
                            {
                                "index":2,
                                "name": "id1",
                                "dataConverters":[
                                    {
                                        "type": "BigInt2Date"
                                    }
                                ]
                            },
                            {
                                "index":2,
                                "name": "id2",
                                "dataConverters":[
                                    {
                                        "type": "BigInt2Date"
                                    },
                                    {
                                        "type": "Date2Str",
                                        "param": {
                                            "pattern":"yyyy-MM"
                                        }
                                    }
                                ]
                            },
                            {
                                "index":3,
                                "name": "update_date1",
                                "dataConverters":[
                                    {
                                        "type": "IfNull2Default",
                                        "param": {
                                            "default": "2021-07-01",
                                            "dataConverters": [
                                                {
                                                    "type": "Str2Date",
                                                    "param": {
                                                        "pattern":"yyyy-MM-dd"
                                                    }
                                                },
                                                {
                                                    "type": "Date2Long"
                                                }
                                            ]
                                        }
                                    }
                                ]
                            },
                            {
                                "index":4,
                                "name": "date_str1",
                                "dataConverters":[
                                    {
                                        "type": "IfNull2Default",
                                        "param": {
                                            "default":"20210801",
                                            "dataConverters": [
                                                {
                                                    "type": "IfElse",
                                                    "param": {
                                                        "if":"if(value=='20210801'){return true;}else{return false;}",
                                                        "value": "return a;",
                                                        "else": "return 4321;",
                                                        "sharedPool": "var a = 10;"
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        ],
                        "sdkDataAddress": "/datax/datax/logag/log",
                        "isGenerateLog":true,
                        "track": {
                            "distinctIdColumn": "id1",
                            "eventName": "testEventName",
                            "isLoginId": true
                        },
                        "type": "item",
                        "user": {
                            "distinctIdColumn": "id1",
                            "isLoginId": true
                        }
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

​		`sdkDataAddress`：数据存放路径，神策分析将接收到的数据经过转换后将数据存放的路径或者神策系统接收数据的url地址。

​		```isGenerateLog```：是否生成神策json文件，默认值true,如生成文件则需要配合神策导入工具使用，不生成文件时，```sdkDataAddress```参数为神策系统接收数据的url地址，如http://localhost:8106/sa?project=default，在神策系统中可查看。

​		`type`：导入神策分析的数据类型，可取值有：track/user/item，分别对应神策的事件/用户/属性。

​		`column`：导入神策分析的属性列表。

​		`saColumn.index`：该列使用读插件字段的下标索引，从0开始。

​		`saColumn.name`：导入神策分析的属性名称。

​		```saColumn.ifNullGiveUp```：当该列值经过转换器转换后为空时，是否丢弃该行数据，默认值为false。

​		`saColumn.dataConverters`：将dataX读出的数据转换为其他类型或者值，所使用的数据转换器。插件支持的数据转换器见下文``内置数据转换器``，支持多个联合使用。

​		`saColumn.dataConverters.type`：使用内置转换器的名称，见下文``内置数据转换器`` ``转换器type``列。

​		`saColumn.dataConverters.param`：使用内置转换器时，转换器必要的参数列表，参数key根据不同转换器不同而不同，见下文``内置数据转换器参数说明``。

​		`track.distinctIdColumn`：type为track时，作为神策distinctId的列，该属性应该在saColumn列表中，并且该属性不能存在空值。

​		`track.eventName`：type为track时，导入神策分析的事件名，当该列值为动态的在```saColumn```中时，可将```saColumn```中的列名改为```eventEventName```（该列不能存在空值），即可达到动态的效果。

​		`track.isLoginId`：type为track时，作为神策distinctId的列是否是登录ID，即用户的唯一标识，布尔值，当该列值为动态的在```saColumn```中时，可将```saColumn```中的列名改为```eventIsLoginId```（该列不能存在空值），即可达到动态的效果。

​		`user.distinctIdColumn`：type为user时，作为神策distinctId的列，该属性应该在saColumn列表中，并且该属性不能存在空值。

​		`user.isLoginId`：type为user时，作为神策distinctId的列是否是登录ID，即用户的唯一标识，布尔值，当该列值为动态的在```saColumn```中时，可将```saColumn```中的列名改为```userIsLoginId```（该列不能存在空值），即可达到动态的效果。

​		`item.itemIdColumn`：type为item时，作为神策itemId的列，该属性应该在saColumn列表中，并且该属性不能存在空值。

​		`item.itemType`：type为item时，作为神策itemType的列，如该配置项的值在saColumn列表中，该属性不能存在空值并且`item.typeIsColumn`配置项应该为`true`，否则将以常量值作为神策itemType。

​		`item.typeIsColumn`：type为item时，`item.itemType`配置项是否在`saColumn`配置项的列表中。

​		```plugin```：神策写插件的插件列表数组，开发规范见**神策写插件插件规范**。

​		```plugin.name```：插件的名称。

​		```plugin.className```：插件的全限定名。

​		```plugin.param```：插件所需要的参数，具体参数根据插件不同而不同。



## **神策写插件插件规范**

​			引入插件机制目的：神策分析支持可变事件，在当前写插件的基础上是无法实现通用的实现方案，所以提供插件机制以支持可变事件，当然不仅仅是可变事件，其他定制化开发也是可以的，神策内部已实现使用redis实现通用可变事件，如需要，请联系神策开发人员。

- ​	引入common依赖

  ```xml
  <dependency>
      <groupId>cn.sensorsdata</groupId>
      <artifactId>plugin-sa-history-datax-writer-common-plugin</artifactId>
      <version>1.0-SNAPSHOT</version>
  </dependency>
  ```

- 编写代码

  继承com.alibaba.BasePlugin类，重写instance方法（配置文件中plugin.param的配置项会被传递到该方法中），以及定义内部类继承com.alibaba.BasePlugin的内部类BasePlugin.SAPlugin，重写process方法。

- 部署插件

  将插件连同依赖一起打包生成jar包，在datax的```sahistorywriter```插件下新建plugin文件夹，然后再新建一个放置该插件的文件夹，命名无要求，配置文件中```plugin.name```参数为该文件夹名，最后将生成的jar包放置到该文件夹下。

  ***实现原理***

  神策写插件会实例化该类，并调用instance方法获取到BasePlugin.SAPlugin插件实例，然后调用SAPlugin的process方法（经过转换器转换后的值会被传递到该方法中，空值将会被丢弃）。

## **类型转换**

|    dataX     |       插件类型       |
| :----------: | :------------------: |
| StringColumn |   java.lang.String   |
|  BoolColumn  |  java.long.Boolean   |
|  LongColumn  | java.math.BigInteger |
| DoubleColumn | java.math.BigDecimal |
|  DateColumn  |    java.util.Date    |
|     null     |         丢弃         |

## 内置数据转换器

|   转换器type   |        转换器全称        |                             功能                             |
| :------------: | :----------------------: | :----------------------------------------------------------: |
|   Long2Date    |    Long2DateConverter    |                  将long转换为java.util.Date                  |
|    Date2Str    |    Date2StrConverter     |            将java.util.Date转换为java.long.String            |
|   Date2Long    |    Date2LongConverter    |                  将java.util.Date转换为long                  |
|   Number2Str   |   Number2StrConverter    |                将数值型转换为java.long.String                |
|    Str2Long    |    Str2LongConverter     |                 将java.long.String转换为long                 |
|    Str2Date    |    Str2DateConverter     |            将java.long.String转换为java.util.Date            |
|  BigInt2Date   | BigInteger2DateConverter |          将java.math.BigInteger转换为java.util.Date          |
|    Str2Int     |     Str2IntConverter     |          将java.long.String转换为java.long.Integer           |
|   Str2Double   |   Str2DoubleConverter    |           将java.long.String转换为java.long.Double           |
| Str2BigDecimal | Str2BigDecimalConverter  |         将java.long.String转换为java.math.BigDecimal         |
| IfNull2Default | IfNull2DefaultConverter  | 将null或者空串的值转换为给定的默认值，支持默认值再转换为其他类型 |
|  NotNull2Null  |  NotNull2NullConverter   |                   将不为null的值转换为null                   |
|     IfElse     |     IfElseConverter      | if表达式条件成立返回特定表达式值，否则返回else表达式值，使用JavaScript引擎解析 |
| IfNull2Column  |  IfNull2ColumnConverter  | 如果该列的值为空，则取该转换器配置的列（注意：该转换器配置的列必须配置在该列之前） |



## 内置数据转换器参数说明

### Long2Date

​	无参数要求

#### 示例

```json
"dataConverters":[
    {
        "type": "BigInt2Date"
    }
]
```

### Date2Str

```pattern```：转换为字符串的时间格式

#### 示例

```json
"dataConverters":[
    {
        "type": "Date2Str",
        "param": {
            "pattern":"yyyy-MM-dd"
        }
    }
]
```



### Date2Long

无参数要求

#### 示例

```json
"dataConverters":[
    {
        "type": "Date2Long"
    }
]
```

### Number2Str

无参数要求

#### 示例

```json
"dataConverters":[
    {
        "type": "Number2Str"
    }
]
```

### Str2Long

无参数要求

#### 示例

```json
"dataConverters":[
    {
        "type": "Str2Long"
    }
]
```

### Str2Date

```pattern```：字符串的时间格式，非必须，内置了时间格式：yyyy-MM-dd、yyyy-MM-dd HH:mm:ss、yyyy-MM-dd HH:mm:ss.SSS、yyyy-MM、yyyyMM、yyyyMMdd、yyyyMMddHHmmss、yyyyMMddHHmmssSSS

```formats```：其他格式的字符串时间格式数组（数据来源的时间格式可能存在多种，可以弥补pattern的不足）

#### 示例

```json
"dataConverters":[
    {
        "type": "Str2Date",
        "param": {
            "pattern":"yyyy-MM-dd",
            "formats":["yyyyMMdd","yyyyMMddHHmmss"]
        }
    }
]
```

### BigInt2Date

无参数要求

#### 示例

```json
"dataConverters":[
    {
        "type": "BigInt2Date"
    }
]
```

### Str2Int

无参数要求

#### 示例

```json
"dataConverters":[
    {
        "type": "Str2Int"
    }
]
```

### Str2Double

无参数要求

#### 示例

```json
"dataConverters":[
    {
        "type": "Str2Double"
    }
]
```

### Str2BigDecimal

无参数要求

#### 示例

```json
"dataConverters":[
    {
        "type": "Str2BigDecimal"
    }
]
```

### IfNull2Default

```default```：默认值

```dataConverters```：数据转换器，内置的任意的数据转换器，将```default```参数给定的默认值转换为其他类型

```dataConverters.type```：数据转换器的类型

```dataConverters.param```：数据转换器的参数

#### 示例

如果该列的值为null或者空字符串，那么设置该值为字符串类型的`2021-07-01`，并且将该字符串转换为`yyyy-MM-dd`格式的时间，再将该时间转换为long型的毫秒值。

```json
"dataConverters":[
    {
        "type": "IfNull2Default",
        "param": {
            "default": "2021-07-01",
            "dataConverters": [
                {
                    "type": "Str2Date",
                    "param": {
                        "pattern":"yyyy-MM-dd"
                    }
                },
                {
                    "type": "Date2Long"
                }
            ]
        }
    }
]
```

### NotNull2Null

无参数要求

#### 示例

```json
"dataConverters":[
    {
        "type": "NotNull2Null"
    }
]
```

### IfElse

```if```：if条件表达式，使用JavaScript引擎解析，确保返回值为boolean类型，导入神策分析的当前列名，和当前读插件获取到的value值，以及dataConverters下配置的param参数会传递到```if```表达式中,可通过`targetColumnName`、`value`、`param`分别获取对应的值

```value```：if条件表达式成立时，返回转换后的值，使用JavaScript引擎解析，导入神策分析的当前列名，和当前读插件获取到的value值，以及dataConverters下配置的param参数会传递到```value```表达式中,可通过`targetColumnName`、`value`、`param`分别获取对应的值

```else```：if条件表达式不成立时，返回转换后的值，使用JavaScript引擎解析，导入神策分析的当前列名，和当前读插件获取到的value值，以及dataConverters下配置的param参数会传递到```else```表达式中,可通过`targetColumnName`、`value`、`param`分别获取对应的值

```sharedPool```：共享区，使用JavaScript引擎解析，该值定义的变量或常量在```if```、```value```、```else```中都能使用

#### 示例

```json
"dataConverters": [
    {
        "type": "IfElse",
        "param": {
            "if":"if(value=='20210801'){return true;}else{return false;}",
            "value": "return a;",
            "else": "return 4321;",
            "sharedPool": "var a = 10;"
        }
    }
]
```

### IfNull2Column

```targetColumnName```：导入神策系统的列名

功能：如果该列为空，则转向取```targetColumnName```配置项的列。

注意：targetColumnName配置的列应该在应用IfNull2Column转换器的列之前配置。

#### 示例

```json
"dataConverters": [
    {
        "type": "IfNull2Column",
        "param": {
            "targetColumnName":"age1"
        }
    }
]
```



