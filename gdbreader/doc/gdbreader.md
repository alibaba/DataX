
# DataX GDBReader

## 1. 快速介绍

GDBReader插件实现读取GDB实例数据的功能，通过`Gremlin Client`连接远程GDB实例，按配置提供的`label`生成查询DSL，遍历点或边数据，包括属性数据，并将数据写入到Record中给到Writer使用。

## 2. 实现原理

GDBReader使用`Gremlin Client`连接GDB实例，按`label`分不同Task取点或边数据。
单个Task中按`label`遍历点或边的id，再切分范围分多次请求查询点或边和属性数据，最后将点或边数据根据配置转换成指定格式记录发送给下游写插件。

GDBReader按`label`切分多个Task并发，同一个`label`的数据批量异步获取来加快读取速度。如果配置读取的`label`列表为空，任务启动前会从GDB查询所有`label`再切分Task。

## 3. 功能说明

GDB中点和边不同，读取需要区分点和边点配置。

### 3.1 点配置样例

```
{
  "job": {
    "setting": {
      "speed": {
        "channel": 1
      }
      "errorLimit": {
        "record": 1
      }
    },

    "content": [
      {
        "reader": {
          "name": "gdbreader",
          "parameter": {
            "host": "10.218.145.24",
            "port": 8182,
            "username": "***",
            "password": "***",
            "fetchBatchSize": 100,
            "rangeSplitSize": 1000,
            "labelType": "VERTEX",
            "labels": ["label1", "label2"],
            "column": [
              {
                "name": "id",
                "type": "string",
                "columnType": "primaryKey"
              },
              {
                "name": "label",
                "type": "string",
                "columnType": "primaryLabel"
              },
              {
                "name": "age",
                "type": "int",
                "columnType": "vertexProperty"
              }
            ]
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

### 3.2 边配置样例

```
{
  "job": {
    "setting": {
      "speed": {
        "channel": 1
      },
      "errorLimit": {
        "record": 1
      }
    },

    "content": [
      {
        "reader": {
          "name": "gdbreader",
          "parameter": {
            "host": "10.218.145.24",
            "port": 8182,
            "username": "***",
            "password": "***",
            "fetchBatchSize": 100,
            "rangeSplitSize": 1000,
            "labelType": "EDGE",
            "labels": ["label1", "label2"],
            "column": [
              {
                "name": "id",
                "type": "string",
                "columnType": "primaryKey"
              },
              {
                "name": "label",
                "type": "string",
                "columnType": "primaryLabel"
              },
              {
                "name": "srcId",
                "type": "string",
                "columnType": "srcPrimaryKey"
              },
              {
                "name": "srcLabel",
                "type": "string",
                "columnType": "srcPrimaryLabel"
              },
              {
                "name": "dstId",
                "type": "string",
                "columnType": "srcPrimaryKey"
              },
              {
                "name": "dstLabel",
                "type": "string",
                "columnType": "srcPrimaryLabel"
              },
              {
                "name": "name",
                "type": "string",
                "columnType": "edgeProperty"
              },
              {
                "name": "weight",
                "type": "double",
                "columnType": "edgeProperty"
              }
            ]
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

### 3.3 参数说明

* **host**
  * 描述：GDB实例连接地址，对应'实例管理'->'基本信息'页面的网络地址
  * 必选：是
  * 默认值：无

* **port**
  * 描述：GDB实例连接地址对应的端口
  * 必选：是
  * 默认值：8182

* **username**
  * 描述：GDB实例账号名
  * 必选：是
  * 默认值：无

* **password**
  * 描述：GDB实例账号名对应的密码
  * 必选：是
  * 默认值：无

* **fetchBatchSize**
  * 描述：一次GDB请求读取点或边的数量，响应包含点或边以及属性
  * 必选：是
  * 默认值：100

* **rangeSplitSize**
  * 描述：id遍历，一次遍历请求扫描的id个数
  * 必选：是
  * 默认值：10 \* fetchBatchSize

* **labels**
  * 描述：标签数组，即需要导出的点或边标签，支持读取多个标签，用数组表示。如果留空([])，表示GDB中所有点或边标签
  * 必选：是
  * 默认值：无

* **labelType**
  * 描述：数据标签类型，支持点、边两种枚举值
    * VERTEX：表示点
    * EDGE：表示边
  * 必选：是
  * 默认值：无

* **column**
  * 描述：点或边字段映射关系配置
  * 必选：是
  * 默认值：无

* **column -> name**
  * 描述：点或边映射关系的字段名，指定属性时表示读取的属性名，读取其他字段时会被忽略
  * 必选：是
  * 默认值：无

* **column -> type**
  * 描述：点或边映射关系的字段类型
    * id, label在GDB中都是string类型，配置非string类型时可能会转换失败
    * 普通属性支持基础类型，包括int, long, float, double, boolean, string
    * GDBReader尽量将读取到的数据转换成配置要求的类型，但转换失败会导致该条记录错误
  * 必选：是
  * 默认值：无

* **column -> columnType**
  * 描述：GDB点或边数据到列数据的映射关系，支持以下枚举值：
    * primaryKey： 表示该字段是点或边的id
    * primaryLabel： 表示该字段是点或边的label
    * srcPrimaryKey： 表示该字段是边关联的起点id，只在读取边时使用
    * srcPrimaryLabel： 表示该字段是边关联的起点label，只在读取边时使用
    * dstPrimaryKey： 表示该字段是边关联的终点id，只在读取边时使用
    * dstPrimaryLabel： 表示该字段是边关联的终点label，只在读取边时使用
    * vertexProperty： 表示该字段是点的属性，只在读取点时使用，应用到SET属性时只读取其中的一个属性值
    * vertexJsonProperty： 表示该字段是点的属性集合，只在读取点时使用。属性集合使用JSON格式输出，包含所有的属性，不能与其他vertexProperty配置一起使用
    * edgeProperty： 表示该字段是边的属性，只在读取边时使用
    * edgeJsonProperty： 表示该字段是边的属性集合，只在读取边时使用。属性集合使用JSON格式输出，包含所有的属性，不能与其他edgeProperty配置一起使用
  * 必选：是
  * 默认值：无
  * vertexJsonProperty格式示例，新增`c`字段区分SET属性，但是SET属性只包含单个属性值时会标记成普通属性
  ```
  {"properties":[
    {"k":"name","t","string","v":"Jack","c":"set"},
    {"k":"name","t","string","v":"Luck","c":"set"},
    {"k":"age","t","int","v":"20","c":"single"}
  ]}
  ```
  * edgeJsonProperty格式示例，边不支持多值属性
  ```
  {"properties":[
    {"k":"created_at","t","long","v":"153498653"},
    {"k":"weight","t","double","v":"3.14"}
  ]}

## 4 性能报告
(TODO)

## 5 使用约束
无

## 6 FAQ
无

