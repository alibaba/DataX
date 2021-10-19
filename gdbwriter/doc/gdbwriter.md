# DataX GDBWriter

## 1 快速介绍

GDBWriter插件实现了写入数据到GDB实例的功能。GDBWriter通过`Gremlin Client`连接远程GDB实例，获取Reader的数据，生成写入DSL语句，将数据写入到GDB。

## 2 实现原理

GDBWriter通过DataX框架获取Reader生成的协议数据，使用`g.addV/E(GDB___label).property(id, GDB___id).property(GDB___PK1, GDB___PV1)...`语句写入数据到GDB实例。

可以配置`Gremlin Client`工作在session模式，由客户端控制事务，在一次事务中实现多个记录的批量写入。

## 3 功能说明
因为GDB中点和边的配置不同，导入时需要区分点和边的配置。

### 3.1 点配置样例
* 这里是一份从内存生成点数据导入GDB实例的配置

```json
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "streamreader",
                    "parameter": {
                        "column" : [
                            {
                                "random": "1,100",
                                "type": "double"
                            },
                            {
                                "random": "1000,1200",
                                "type": "long"
                            },
                            {
                                "random": "60,64",
                                "type": "string"
                            },
                            {
                                "random": "100,1000",
                                "type": "long"
                            },
                            {
                                "random": "32,48",
                                "type": "string"
                            }
                        ],
                        "sliceRecordCount": 1000
                    }
                },
                "writer": {
                    "name": "gdbwriter",
                    "parameter": {
                        "host": "gdb-endpoint",
                        "port": 8182,
                        "username": "root",
                        "password": "***",
                        "writeMode": "INSERT",
                        "labelType": "VERTEX",
                        "label": "#{1}",
                        "idTransRule": "none",
                        "session": true,
                        "maxRecordsInBatch": 64,
                        "column": [
                            {
                                "name": "id",
                                "value": "#{0}",
                                "type": "string",
                                "columnType": "primaryKey"
                            },
                            {
                                "name": "vertex_propKey",
                                "value": "#{2}",
                                "type": "string",
                                "columnType": "vertexSetProperty"
                            },
                            {
                                "name": "vertex_propKey",
                                "value": "#{3}",
                                "type": "long",
                                "columnType": "vertexSetProperty"
                            },
                            {
                                "name": "vertex_propKey2",
                                "value": "#{4}",
                                "type": "string",
                                "columnType": "vertexProperty"
                            }
                        ]
                    }
                }
            }
        ]
    }
}

```
### 3.2 边配置样例
* 这里是一份从内存生成边数据导入GDB实例的配置
> **注意**
> 下面配置导入边时，需要提前在GDB实例中写入点，要求分别存在id为`person-{{i}}`和`book-{{i}}`的点，其中i取值0~100。

```json

{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "streamreader",
                    "parameter": {
                        "column" : [
                            {
                                "random": "100,200",
                                "type": "double"
                            },
                            {
                                "random": "1,100",
                                "type": "long"
                            },
                            {
                                "random": "1,100",
                                "type": "long"
                            },
                            {
                                "random": "2000,2200",
                                "type": "long"
                            },
                            {
                                "random": "60,64",
                                "type": "string"
                            }
                        ],
                        "sliceRecordCount": 1000
                    }
                },
                "writer": {
                    "name": "gdbwriter",
                    "parameter": {
                        "host": "gdb-endpoint",
                        "port": 8182,
                        "username": "root",
                        "password": "***",
                        "writeMode": "INSERT",
                        "labelType": "EDGE",
                        "label": "#{3}",
                        "idTransRule": "none",
                        "srcIdTransRule": "labelPrefix",
                        "dstIdTransRule": "labelPrefix",
                        "srcLabel":"person-",
                        "dstLabel":"book-",
                        "session":false,
                        "column": [
                            {
                                "name": "id",
                                "value": "#{0}",
                                "type": "string",
                                "columnType": "primaryKey"
                            },
                            {
                                "name": "id",
                                "value": "#{1}",
                                "type": "string",
                                "columnType": "srcPrimaryKey"
                            },
                            {
                                "name": "id",
                                "value": "#{2}",
                                "type": "string",
                                "columnType": "dstPrimaryKey"
                            },
                            {
                                "name": "edge_propKey",
                                "value": "#{4}",
                                "type": "string",
                                "columnType": "edgeProperty"
                            }
                        ]
                    }
                }
            }
        ]
    }
}

```

### 3.3 参数说明

* **host**
  * 描述：GDB实例连接域名，对应阿里云控制台->"图数据库 GDB"->"实例管理"->"基本信息" 中的"内网地址"；
  * 必选：是
  * 默认值：无

* **port**
  * 描述：GDB实例连接端口
  * 必选：是
  * 默认值：8182

* **username**
  * 描述：GDB实例账号名
  * 必选：是
  * 默认值：无

* **password**
  * 描述：图实例账号名对应密码
  * 必选：是
  * 默认值：无

* **label**
  * 描述：类型名，即点/边名称； label支持从源列中读取，如#{0}，表示取第一列字段作为label名。源列索引从0开始；
  * 必选：是
  * 默认值：无

* **labelType**
  * 描述：label类型；
    * 枚举值"VERTEX"表示点
    * 枚举值"EDGE"表示边
  * 必选：是
  * 默认值：无

* **srcLabel**
  * 描述：当label为边时，表示起点的点名称；srcLabel支持从源列中读取，如#{0}，表示取第一列字段作为label名。源列索引从0开始；
  * 必选：labelType为边，srcIdTransRule为none时可不填写，否则必填；
  * 默认值：无

* **dstLabel**
  * 描述：当label为边时，表示终点的点名称；dstLabel支持从源列中读取，如#{0}，表示取第一列字段作为label名。源列索引从0开始；
  * 必选：labelType为边，dstIdTransRule为none时可不填写，否则必填；
  * 默认值：无

* **writeMode**
  * 描述：导入id重复时的处理模式；
    * 枚举值"INSERT"表示会报错，错误记录数加1；
    * 枚举值"MERGE"表示更新属性值，不计入错误；
    * 枚举值"SKIP"表示跳过，不计入错误
  * 必选：是
  * 默认值：INSERT

* **idTransRule**
  * 描述：主键id转换规则；
    * 枚举值"labelPrefix"表示将映射的值转换为{label名}{源字段}
    * 枚举值"none"表示映射的值不做转换
  * 必选：是
  * 默认值："none"

* **srcIdTransRule**
  * 描述：当label为边时，表示起点的主键id转换规则；
    * 枚举值"labelPrefix"表示映射的值转换为为{label名}{源字段}
    * 枚举值"none"表示映射的值不做转换，此时srcLabel 可不填写
  * 必选：label为边时必选
  * 默认值："none"

* **dstIdTransRule**
  * 描述：当label为边时，表示终点的主键id转换规则；
    * 枚举值"labelPrefix"表示映射的值转换为为{label名}{源字段}
    * 枚举值"none"表示映射的值不做转换，此时dstLabel 可不填写
  * 必选：label为边时必选
  * 默认值："none"

* **session**
  * 描述：是否使用`Gremlin Client`的session模式写入数据
  * 必选：否
  * 默认值：false
  
* **maxRecordsInBatch**
  * 描述：使用`Gremlin Client`的session模式时，一次事务处理的记录数
  * 必选：否
  * 默认值：16
  
* **column**
  * 描述：点/边字段映射关系配置
  * 必选：是
  * 默认值：无

* **column -> name**
  * 描述：点/边映射关系的字段名
  * 必选：是
  * 默认值：无

* **column -> value**
  * 描述：点/边映射关系的字段值；
    * #{N}表示直接映射源端值，N为源端column索引，从0开始；#{0}表示映射源端column第1个字段；
    * test-#{0} 表示源端值做拼接转换，#{0}值前/后可添加固定字符串;
    * #{0}-#{1}表示做多字段拼接，也可在任意位置添加固定字符串，如test-#{0}-test1-#{1}-test2
  * 必选：是
  * 默认值：无

* **column -> type**
  * 描述：点/边映射关系的字段值类型；
    * 主键id只支持string类型，GDBWriter插件会强制转换，源id必须保证可转换为string；
    * 普通属性支持类型：int, long, float, double, boolean, string
  * 必选：是
  * 默认值：无

* **column -> columnType**
  * 描述：点/边映射关系字段对应到GDB点/边数据的类型，支持以下几类枚举值：
    * 公共枚举值：
      * primaryKey：表示该字段是主键id
    * 点枚举值：
      * vertexProperty：labelType为点时，表示该字段是点的普通属性
      * vertexSetProperty：labelType为点时，表示该字段是点的SET属性，value是SET属性中的一个属性值
      * vertexJsonProperty：labelType为点时，表示是点json属性，value结构请见备注**json properties示例**，点配置最多只允许出现一个json属性；
    * 边枚举值：
      * srcPrimaryKey：labelType为边时，表示该字段是起点主键id
      * dstPrimaryKey：labelType为边时，表示该字段是终点主键id
      * edgeProperty：labelType为边时，表示该字段是边的普通属性
      * edgeJsonProperty：labelType为边时，表示是边json属性，value结构请见备注**json properties示例**，边配置最多只允许出现一个json属性；
  * 必选：是
  * 默认值：无
  * 备注：**json properties示例**
  > ```json
  > {"properties":[
  >    {"k":"name","t":"string","v":"tom"},
  >    {"k":"age","t":"int","v":"20"},
  >    {"k":"sex","t":"string","v":"male"}
  > ]}
  >
  > # json格式同样支持给点添加SET属性，格式如下
  > {"properties":[
  >    {"k":"name","t":"string","v":"tom","c":"set"},
  >    {"k":"name","t":"string","v":"jack","c":"set"},
  >    {"k":"age","t":"int","v":"20"},
  >    {"k":"sex","t":"string","v":"male"}
  > ]}
  > ```

## 4 性能报告
### 4.1 环境参数
GDB实例规格
- 16core 128GB, 1TB SSD

DataX压测机器
- cpu: 4 * Intel(R) Xeon(R) Platinum 8163 CPU @ 2.50GHz
- mem: 16GB
- net: 千兆双网卡
- os: CentOS 7, 3.10.0-957.5.1.el7.x86_64
- jvm: -Xms4g -Xmx4g

### 4.2 数据特征

```
{
    id: random double(1~10000)
    from: random long(1~40000000)
    to: random long(1~40000000)
    label: random long(20000000 ~ 20005000)
    propertyKey: random string(len: 120~128)
    propertyName: random string(len: 120~128)
}
```
- 点/边都有一个属性，属性key和value都是长度120~128字节的随机字符串
- label是范围20000000 ~ 20005000的随机整数转换的字符串
- id是浮点数转换的字符串，防止重复
- 边包含关联起点和终点，测试边时已经提前导入twitter数据集的点数据(4200W)

### 4.3 任务配置
分点和边的配置，具体配置与上述的示例配置相似，下面列出关键的差异点

- 增加并发任务数量
> "channel": 32

- 使用session模式
> "session": true

- 增加事务批量处理记录个数
> "maxRecordsInBatch": 128

### 4.4 测试结果

点导入性能：
- 任务平均流量： 4.07MB/s
- 任务总计耗时： 412s
- 记录写入速度： 15609rec/s
- 读出记录总数： 6400000

边导入性能：
- 任务平均流量： 2.76MB/s
- 任务总计耗时： 1602s
- 记录写入速度： 10000rec/s
- 读出记录总数： 16000000

## 5 约束限制
- 导入边记录前要求GDB中已经存在边关联的起点/终点
- GDBWriter插件与用户查询DSL使用相同的GDB实例端口，导入时可能会影响查询性能

## FAQ
1. 使用SET属性需要升级GDB实例到`1.0.20`版本及以上。
2. 边只支持普通单值属性，不能给边写SET属性数据。
