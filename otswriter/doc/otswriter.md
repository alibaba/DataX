
# OTSWriter 插件文档


___


## 1 快速介绍

OTSWriter插件实现了向OTS写入数据，目前支持了多版本数据的写入、主键自增列的写入等功能。


OTS是构建在阿里云飞天分布式系统之上的 NoSQL数据库服务，提供海量结构化数据的存储和实时访问。OTS 以实例和表的形式组织数据，通过数据分片和负载均衡技术，实现规模上的无缝扩展。

## 2 实现原理

简而言之，OTSWriter通过OTS官方Java SDK连接到OTS服务端，并通过SDK写入OTS服务端。OTSWriter本身对于写入过程做了很多优化，包括写入超时重试、异常写入重试、批量提交等Feature。


## 3 功能说明

### 3.1 配置样例

* 配置一个写入OTS作业:

`normal模式`
```
{
    "job": {
        "setting": {
        },
        "content": [
            {
                "reader": {},
                "writer": {
                   "name": "otswriter",
                    "parameter": {
                        "endpoint":"",
                        "accessId":"",
                        "accessKey":"",
                        "instanceName":"",
                        "table":"",
                        
                        // 可选 multiVersion||normal,可选配置，默认normal
                        "mode":"normal",
                        
                        //newVersion定义是否使用新版本插件 可选值：true || false    
                        "newVersion":"true",
                        
                        //是否允许向包含主键自增列的ots表中写入数据
                        //与mode:multiVersion的多版本模式不兼容
                        "enableAutoIncrement":"true",
    
                        // 需要导入的PK列名，区分大小写
                        // 类型支持：STRING，INT,BINARY
                        // 必选
                        // 1. 支持类型转换，注意类型转换时的精度丢失
                        // 2. 顺序不要求和表的Meta一致
                        // 3. name全局唯一
                        "primaryKey":[
                            "userid",
                            "groupid"
                        ],
    
                        // 需要导入的列名，区分大小写
                        // 类型支持STRING，INT，DOUBLE，BOOL和BINARY
                        // 必选
                        // 1.name全局唯一
                        "column":[
                            {"name":"addr", "type":"string"},
                            {"name":"height", "type":"int"}
                        ],
    
                        // 如果用户配置了时间戳，系统将使用配置的时间戳，如果没有配置，使用OTS的系统时间戳
                        // 可选
                        "defaultTimestampInMillionSecond": 142722431,
    
                        // 写入OTS的方式
                        // PutRow : 等同于OTS API中PutRow操作，检查条件是ignore
                        // UpdateRow : 等同于OTS API中UpdateRow操作，检查条件是ignore
                        "writeMode":"PutRow"
                            
                    }
                }
            }
        ]
    }
}
```


### 3.2 参数说明

* **endpoint**

    * 描述：OTS Server的EndPoint(服务地址)，例如http://bazhen.cn−hangzhou.ots.aliyuncs.com。

    * 必选：是 <br />

    * 默认值：无 <br />

* **accessId**

    * 描述：OTS的accessId <br />

    * 必选：是 <br />

    * 默认值：无 <br />

* **accessKey**

    * 描述：OTS的accessKey  <br />

    * 必选：是 <br />

    * 默认值：无 <br />

* **instanceName**

    * 描述：OTS的实例名称，实例是用户使用和管理 OTS 服务的实体，用户在开通 OTS 服务之后，需要通过管理控制台来创建实例，然后在实例内进行表的创建和管理。实例是 OTS 资源管理的基础单元，OTS 对应用程序的访问控制和资源计量都在实例级别完成。 <br />

    * 必选：是 <br />

    * 默认值：无 <br />


* **table**

    * 描述：所选取的需要抽取的表名称，这里有且只能填写一张表。在OTS不存在多表同步的需求。<br />

    * 必选：是 <br />

    * 默认值：无 <br />

* **newVersion**

    * 描述：version定义了使用的ots SDK版本。<br />
        * true，新版本插件，使用com.alicloud.openservices.tablestore的依赖（推荐）
        * false，旧版本插件，使用com.aliyun.openservices.ots的依赖，**不支持多版本数据的读取**

    * 必选：否 <br />

    * 默认值：false <br />

* **mode**

    * 描述：是否为多版本数据，目前有两种模式。<br />
        * normal，对应普通的数据
        * multiVersion，写入数据为多版本格式的数据，多版本模式下，配置参数有所不同，详见3.4节

    * 必选：否 <br />

    * 默认值：normal <br />


* **enableAutoIncrement**

    * 描述：是否允许向包含主键自增列的ots表中写入数据。<br />
        * true，插件会扫描表中的自增列信息，并在写入数据时自动添加自增列
        * false，写入含主键自增列的表时会报错

    * 必选：否 <br />

    * 默认值：false <br />


* **isTimeseriesTable**

    * 描述：写入的对应表是否为时序表，仅在mode=normal模式下生效。<br />
        * true，写入的数据表为时序数据表
        * false，写入的数据表为普通的宽表

    * 必选：否 <br />

    * 默认值：false <br />
  
    * 在写入时序数据表的模式下，不需要配置`primaryKey`字段，只需要配置`column`字段，配置样例:
      ```json
      "column": [
              {
                "name": "_m_name", // 表示度量名称（measurement）字段
              },
              {
                "name": "_data_source", // 表示数据源（dataSource）字段
              },
              {
                "name": "_tags", // 表示标签字段，会被解析为Map<String,String>类型
              },
              {
                "name": "_time", // 表示时间戳字段，会被解析为long类型的值
              },
              {
                "name": "tag_a",  
                "isTag":"true"    // 表示标签内部字段，该字段会被解析到标签的字典内部
              },
              {
                "name": "column1",  // 属性列名称
                "type": "string"  // 属性列类型，支持 bool string int double binary
              },
              {
                "name": "column2",
                "type": "int"
              }
            ],
      ```

  


* **primaryKey**

    * 描述: OTS的主键信息，使用JSON的数组描述字段信息。OTS本身是NoSQL系统，在OTSWriter导入数据过程中，必须指定相应地字段名称。

      OTS的PrimaryKey只能支持STRING，INT两种类型，因此OTSWriter本身也限定填写上述两种类型。

      DataX本身支持类型转换的，因此对于源头数据非String/Int，OTSWriter会进行数据类型转换。

      配置实例:

      ```json
      "primaryKey":[
                          "userid",
                          "groupid"
                      ]
      ```
    * 必选：是 <br />

    * 默认值：无 <br />

* **column**

    * 描述：所配置的表中需要同步的列名集合，使用JSON的数组描述字段信息。使用格式为

      ```json
          {"name":"col2", "type":"INT"},
      ```

      其中的name指定写入的OTS列名，type指定写入的类型。OTS类型支持STRING，INT，DOUBLE，BOOL和BINARY几种类型 。

      写入过程不支持常量、函数或者自定义表达式。

    * 必选：是 <br />

    * 默认值：无 <br />

* **writeMode**

    * 描述：写入模式，目前支持两种模式，

        * PutRow，对应于OTS API PutRow，插入数据到指定的行，如果该行不存在，则新增一行；若该行存在，则覆盖原有行。

        * UpdateRow，对应于OTS API UpdateRow，更新指定行的数据，如果该行不存在，则新增一行；若该行存在，则根据请求的内容在这一行中新增、修改或者删除指定列的值。


* 必选：是 <br />

* 默认值：无 <br />


### 3.3 类型转换

目前OTSWriter支持所有OTS类型，下面列出OTSWriter针对OTS类型转换列表:


| DataX 内部类型| OTS 数据类型    |
| -------- | -----  |
| Long     |Integer |
| Double   |Double|
| String   |String|
| Boolean  |Boolean|
| Bytes    |Binary |

* 注意，OTS本身不支持日期型类型。应用层一般使用Long报错时间的Unix TimeStamp。

### 3.4 multiVersion模式

#### 3.4.1 模式介绍

multiVersion模式解决了ots数据库中多版本数据的导入问题。支持Hbase的全量数据迁移到OTS

*  注意：这种模式的数据格式比较特殊，该writer需要reader也提供版本的输出
* 当前只有hbase reader 与 ots reader提供这种模式，使用时切记注意
#### 3.4.2 配置样例
```
{
    "job": {
        "setting": {
        },
        "content": [
            {
                "reader": {},
                "writer": {
                "name": "otswriter",
                    "parameter": {
                        "endpoint":"",
                        "accessId":"",
                        "accessKey":"",
                        "instanceName":"",
                        "table":"",
                    
                        // 多版本模式，插件会按照多版本模式去解析所有配置
                        "mode":"multiVersion",
                          
                        "newVersion":"true",
                    
                        // 配置PK信息
                        // 考虑到配置成本，并不需要配置PK在Record（Line）中的位置，要求
                        // Record的格式固定,PK一定在行首，PK之后是columnName，格式如下：
                        // 如：{pk0,pk1,pk2,pk3}, {columnName}, {timestamp}, {value}
                        "primaryKey":[
                            "userid",
                            "groupid"
                        ],
                        
                        // 列名前缀过滤
                        // 描述：hbase导入过来的数据，cf和qulifier共同组成columnName，
                        // OTS并不支持cf，所以需要将cf过滤掉
                        // 注意：
                        // 1.该参数选填，如果没有填写或者值为空字符串，表示不对列名进行过滤。
                        // 2.如果datax传入的数据columnName列不是以前缀开始，则将该Record放入脏数据回收器中
                        "columnNamePrefixFilter":"cf:"
                    }
                }
            }
        ]
    }
}
```


## 4 约束限制

### 4.1 写入幂等性

OTS写入本身是支持幂等性的，也就是使用OTS SDK同一条数据写入OTS系统，一次和多次请求的结果可以理解为一致的。因此对于OTSWriter多次尝试写入同一条数据与写入一条数据结果是等同的。

### 4.2 单任务FailOver

由于OTS写入本身是幂等性的，因此可以支持单任务FailOver。即一旦写入Fail，DataX会重新启动相关子任务进行重试。

## 5 FAQ

* 1.如果使用多版本模式，value为null应该怎么解释？
    * : 表示删除指定的版本
* 2.如果ts列为空怎么办？
    * ：插件记录为垃圾数据
* 3.Record的count和期望不符？
    * : 插件异常终止
* 4.在普通模式下，采用UpdateRow的方式写入数据，如果不指定TS，相同行数的数据怎么写入到OTS中？
    * : 后面的覆盖前面的数据
