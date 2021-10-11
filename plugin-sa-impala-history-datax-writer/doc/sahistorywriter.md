## 快速介绍

```saimpalawriter```是用户将数据导入到impala的插件，目前主要用于导入到神策分析的自定义维度表中。

## **实现原理**

```saimpalawriter	```是通过impala JDBC方式，将数据转换为SQL执行插入语句，如果数据量过大，建议直接使用文件方式导入。```saimpalawriter	```支持四种模式，分别为：``insert``、``insertBatch``、``update``、``insertUpdate``。

- ``insert``模式：是通过生成 insert into 表名 values(值1,值2) SQL执行。
-  ``insertBatch``模式：是通过生成 insert into 表名 values(值1,值2) ,(值1,值2) ,(值1,值2) SQL执行。
- ``update``模式：是通过生成 update  表名 set 字段名1 = 值1, 字段名2 = 值2 where 字段名3 = 值3 SQL执行。
- ``insertUpdate``模式：是``insert``模式和``update``模式相结合，首先尝试insert，失败时执行update。

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
                    "name": "saimpalawriter",
                    "parameter": {
                        "url": "jdbc:impala://10.120.17.177:21050/dimensions",
						"table": "test",
						"model":"insertBatch",
						"batchSize":40,
						"updateWhereColumn":["id"],
                        "column": [
                            {
                                "index":0,
                                "name": "name",
                                "exclude": true
                            },
							{
                                "index":1,
                                "name": "id"
                            },
                            {
                                "index":2,
                                "name": "age",
								"dataConverters":[
									{
										"type": "IfElse",
										"param": {
											"if": "return true;",
											"value":"return value + 1.6;"
										}
									},
									{
										"type": "Number2Long",
										"param": {
											"model": "half_up"
										}
									}
								]
                            },
							{
                                "index":3,
                                "name": "system_code",
								"dataConverters":[
									{
										"type": "NumEnum",
										"param": {
											"enum":{
												"10295":"v1",
												"1":"v2",
												"2.0":"v3"
											},
											"default":"v4",
											"nullValue":"v5",
											"nanValue":"v6"
										}
									}
								]
                            }
                        ]
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

​		`url`：连接impala的JDBC URL。

​		```table```：导入数据的目的表名。

​		`model`：导入数据时执行的模式，可取值有：``insert``、``insertBatch``、``update``、``insertUpdate``。

​		`batchSize`：当``model``为``insertBatch``模式时，批量插入的数量，默认值为500。

​		`		updateWhereColumn`：当``model``为``insertBatch``或者``update``模式时，生成update语句时的where条件列集合，该配置项中的列需要与column配置项的name值相同。建议使用唯一主键。如column配置列有id，name,age，该配置项为["id","name"],则生成的sql为：update 表名 set age = 值1 where id = 值2 and name = 值3，如果从读插件中获取到name的值为空，则where条件中的name为name is null。

​		`column`：导入目的表的列名集合。

​		`column.index`：该列使用读插件字段的下标索引，从0开始。

​		`column.name`：导入目的表的列名。

​		`column.exclude`：是否排除该字段的导入，默认值false。

​		```column.ifNullGiveUp```：当该列值经过转换器转换后为空时，是否丢弃该行数据，默认值为false。

​		`column.dataConverters`：将dataX读出的数据转换为其他类型或者值，所使用的数据转换器。插件支持的数据转换器见下文``内置数据转换器``，支持多个联合使用。

​		``column.dataConverters.type``：使用内置转换器的名称，见下文``内置数据转换器`` ``转换器type``列。

​		`column.dataConverters.param`：使用内置转换器时，转换器必要的参数列表，参数key根据不同转换器不同而不同，见下文``内置数据转换器参数说明``。

## **类型转换**

插件支持的impala数据类型有：string、varchar、char、int、bigint、float、integer、double、decimal、tinyint、smallint、real、boolean、timestamp、datetime、date

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
|  Number2Long   |   Number2LongConverter   | 将数值转换为long数值，支持三种模式。<br />向下取整:默认模式，如值为3.4，转换后为3<br />向上取整：如值为3.6，转换后为4<br />四舍五入：如值为3.6，转换后为4 |
|    StrEnum     |     StrEnumConverter     |                   将字符串的枚举值执行转换                   |
|    NumEnum     |   NumberEnumConverter    |                    将数字的枚举值执行转换                    |
|  BytesArr2Str  |  BytesArr2StrConverter   |                    将byte数组转换为字符串                    |



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

### Number2Long

```model```：转换为整数时的模式

功能：将数字转换为整数，支持三种模式，分别为：``向下取整``模式（默认）、``向上取整``模式、``四舍五入``模式。

``向下取整``：默认模式，可不配置``model``参数。

``向上取整``：``model``参数为：``up``。

``四舍五入``：``model``参数为：``half_up``。

#### 示例

```json
"dataConverters": [
    {
        "type": "IfNull2Column",
        "param": {
            "model":"up"
        }
    }
]
```

### StrEnum

```enum```：转换的枚举键值对列表。

```nullValue```：该列值为null时，填充的值。

```default```：当该列值既不是null，也不在enum中时，填充的值。

功能：将字符串的枚举值执行转换。

#### 示例

```json
"dataConverters": [
    {
        "type": "StrEnum",
        "param": {
            "enum":{
              "student":"学生",
              "teacher":"老师",
              "president":"校长"
            },
            "nullValue":"未知角色",
            "default":"职工"
        }
    }
]
```

### NumEnum

```enum```：转换的枚举键值对列表。

```nullValue```：该列值为null时，填充的值。

```default```：当该列值既不是null，也不在enum中时，填充的值。

```nanValue```：当该列值不能转换为数字时，填充的值。

功能：将数字（支持浮点数）的枚举值执行转换。

#### 示例

```json
"dataConverters": [
    {
        "type": "NumEnum",
        "param": {
             "enum":{
              "01"":"学生",
              "02":"老师",
              "23.5":"校长"
            },
            "nullValue":"未知角色",
            "default":"职工",
            "nanValue":"错误类型"
        }
    }
]
```

### BytesArr2Str

无参数要求

#### 示例

```json
"dataConverters":[
    {
        "type": "BytesArr2Str"
    }
]
```

### 
