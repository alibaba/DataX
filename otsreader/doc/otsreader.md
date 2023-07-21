
# OTSReader 插件文档


___


## 1 快速介绍

OTSReader插件实现了从OTS读取数据，并可以通过用户指定抽取数据范围可方便的实现数据增量抽取的需求。目前支持三种抽取方式：

* 全表抽取
* 范围抽取
* 指定分片抽取

本版本的OTSReader新增了支持多版本数据的读取功能，同时兼容旧版本的配置文件

## 2 实现原理

简而言之，OTSReader通过OTS官方Java SDK连接到OTS服务端，获取并按照DataX官方协议标准转为DataX字段信息传递给下游Writer端。

OTSReader会根据OTS的表范围，按照Datax并发的数目N，将范围等分为N份Task。每个Task都会有一个OTSReader线程来执行。

## 3 功能说明

### 3.1 配置样例

#### 3.1.1
* 配置一个从OTS表读取单版本数据的reader:

```
{
  "job": {
    "setting": {
      "speed": {
        //设置传输速度，单位为byte/s，DataX运行会尽可能达到该速度但是不超过它.
        "byte": 1048576
      }
      //出错限制
      "errorLimit": {
        //出错的record条数上限，当大于该值即报错。
        "record": 0,
        //出错的record百分比上限 1.0表示100%，0.02表示2%
        "percentage": 0.02
      }
    },
    "content": [
      {
        "reader": {
          "name": "otsreader-internal",
          "parameter": {
            "endpoint":"",
            "accessId":"",
            "accessKey":"",
            "instanceName":"",
            "table": "",
            //version定义了是否使用新版本插件 可选值：false || true
            "newVersion":"false",
            //mode定义了读取数据的格式（普通数据/多版本数据），可选值：normal || multiversion
            "mode": "normal",
            
            // 导出的范围,读取的范围是[begin,end)，左闭右开的区间
            // begin小于end，表示正序读取数据
            // begin大于end，表示反序读取数据
            // begin和end不能相等
            // type支持的类型有如下几类：
            //   string、int、binary
            //   binary输入的方式采用二进制的Base64字符串形式传入
            //   INF_MIN 表示无限小
            //   INF_MAX 表示无限大
            "range":{
                // 可选，默认表示从无限小开始读取
                // 这个值的输入可以填写空数组，或者PK前缀，亦或者完整的PK，在正序读取数据时，默认填充PK后缀为INF_MIN，反序为INF_MAX
                // 例子：
                // 如果用户的表有2个PK，类型分别为string、int，那么如下3种输入都是合法，如：
                //   1. []                                --> 表示从表的开始位置读取
                //   2. [{"type":"string", "value":"a"}]  --> 表示从[{"type":"string", "value":"a"},{"type":"INF_MIN"}]
                //   3. [{"type":"string", "value":"a"},{"type":"INF_MIN"}]
                //
                // binary类型的PK列比较特殊，因为Json不支持直接输入二进制数，所以系统定义:用户如果要传入
                // 二进制，必须使用(Java)Base64.encodeBase64String方法，将二进制转换为一个可视化的字符串，然后将这个字符串填入value中
                // 例子(Java)：
                //   byte[] bytes = "hello".getBytes();  # 构造一个二进制数据，这里使用字符串hello的byte值
                //   String inputValue = Base64.encodeBase64String(bytes) # 调用Base64方法，将二进制转换为可视化的字符串
                //   上面的代码执行之后，可以获得inputValue为"aGVsbG8="
                //   最终写入配置：{"type":"binary","value" : "aGVsbG8="}
                
                "begin":[{"type":"string", "value":"a"},{"type":"INF_MIN"}],
                
                // 默认表示读取到无限大结束
                // 这个值得输入可以填写空数组，或者PK前缀，亦或者完整的PK，在正序读取数据时，默认填充PK后缀为INF_MAX，反序为INF_MIN
                // 可选
                "end":[{"type":"string", "value":"a"},{"type":"INF_MAX"}],
                
                // 当前用户数据较多时，需要开启并发导出，Split可以将当前范围的的数据按照切分点切分为多个并发任务
                // 可选
                //   1. split中的输入值只能PK的第一列（分片建），且值的类型必须和PartitionKey一致
                //   2. 值的范围必须在begin和end之间
                //   3. split内部的值必须根据begin和end的正反序关系而递增或者递减
               "split":[{"type":"string", "value":"b"}, {"type":"string", "value":"c"}]
            },
            

            // 指定要导出的列，支持普通列和常量列
            // 格式
            //   普通列格式：{"name":"{your column name}"}
            //   常量列格式：{"type":"", "value":""} , type支持string、int、binary、bool、double
            //   binary类型需要使用base64转换成对应的字符串传入
            // 注意：
            //   1. PK列也是需要用户在下面单独指定
            "column": [
                {"name":"pk1"},   // 普通列，下同
                {"name":"pk2"},
                {"name":"attr1"},
                {"type":"string","value" : ""}  // 指定常量列，下同
                {"type":"int","value" : ""}
                {"type":"double","value" : ""}
                // binary类型的常量列比较特殊，因为Json不支持直接输入二进制数，所以系统定义:用户如果要传入
                // 二进制，必须使用(Java)Base64.encodeBase64String方法，将二进制转换为一个可视化的字符串，然后将这个字符串填入value中
                // 例子(Java)：
                //   byte[] bytes = "hello".getBytes();  # 构造一个二进制数据，这里使用字符串hello的byte值
                //   String inputValue = Base64.encodeBase64String(bytes) # 调用Base64方法，将二进制转换为可视化的字符串
                //   上面的代码执行之后，可以获得inputValue为"aGVsbG8="
                //   最终写入配置：{"type":"binary","value" : "aGVsbG8="}
                
                {"type":"binary","value" : "aGVsbG8="}
            ],
          }
        },
        "writer": {
          //writer类型
          "name": "streamwriter",
          //是否打印内容
          "parameter": {
            "print": true
          }
        }
      }
    ]
  }
}
```

#### 3.1.2
* 配置一个从OTS表读取多版本数据的reader(仅在newVersion == true时支持):

```
{
  "job": {
    "setting": {
      "speed": {
        //设置传输速度，单位为byte/s，DataX运行会尽可能达到该速度但是不超过它.
        "byte": 1048576
      }
      //出错限制
      "errorLimit": {
        //出错的record条数上限，当大于该值即报错。
        "record": 0,
        //出错的record百分比上限 1.0表示100%，0.02表示2%
        "percentage": 0.02
      }
    },
    "content": [
      {
        "reader": {
          "name": "otsreader-internal",
          "parameter": {
            "endpoint":"",
            "accessId":"",
            "accessKey":"",
            "instanceName":"",
            "table": "",
            //version定义了是否使用新版本插件 可选值：false || true
            "newVersion":"true",
            //mode定义了读取数据的格式（普通数据/多版本数据），可选值：normal || multiversion
            "mode": "multiversion",
            
            // 导出的范围，,读取的范围是[begin,end)，左闭右开的区间
            // begin小于end，表示正序读取数据
            // begin大于end，表示反序读取数据
            // begin和end不能相等
            // type支持的类型有如下几类：
            //   string、int、binary
            //   binary输入的方式采用二进制的Base64字符串形式传入
            //   INF_MIN 表示无限小
            //   INF_MAX 表示无限大
            "range":{
                // 可选，默认表示从无限小开始读取
                // 这个值的输入可以填写空数组，或者PK前缀，亦或者完整的PK，在正序读取数据时，默认填充PK后缀为INF_MIN，反序为INF_MAX
                // 例子：
                // 如果用户的表有2个PK，类型分别为string、int，那么如下3种输入都是合法，如：
                //   1. []                                --> 表示从表的开始位置读取
                //   2. [{"type":"string", "value":"a"}]  --> 表示从[{"type":"string", "value":"a"},{"type":"INF_MIN"}]
                //   3. [{"type":"string", "value":"a"},{"type":"INF_MIN"}]
                //
                // binary类型的PK列比较特殊，因为Json不支持直接输入二进制数，所以系统定义:用户如果要传入
                // 二进制，必须使用(Java)Base64.encodeBase64String方法，将二进制转换为一个可视化的字符串，然后将这个字符串填入value中
                // 例子(Java)：
                //   byte[] bytes = "hello".getBytes();  # 构造一个二进制数据，这里使用字符串hello的byte值
                //   String inputValue = Base64.encodeBase64String(bytes) # 调用Base64方法，将二进制转换为可视化的字符串
                //   上面的代码执行之后，可以获得inputValue为"aGVsbG8="
                //   最终写入配置：{"type":"binary","value" : "aGVsbG8="}
                
                "begin":[{"type":"string", "value":"a"},{"type":"INF_MIN"}],
                
                // 默认表示读取到无限大结束
                // 这个值得输入可以填写空数组，或者PK前缀，亦或者完整的PK，在正序读取数据时，默认填充PK后缀为INF_MAX，反序为INF_MIN
                // 可选
                "end":[{"type":"string", "value":"g"},{"type":"INF_MAX"}],
                
                // 当前用户数据较多时，需要开启并发导出，Split可以将当前范围的的数据按照切分点切分为多个并发任务
                // 可选
                //   1. split中的输入值只能PK的第一列（分片建），且值的类型必须和PartitionKey一致
                //   2. 值的范围必须在begin和end之间
                //   3. split内部的值必须根据begin和end的正反序关系而递增或者递减
                "split":[{"type":"string", "value":"b"}, {"type":"string", "value":"c"}]
            },
            
            // 指定要导出的列，在多版本模式下只支持普通列
            // 格式：
            //   普通列格式：{"name":"{your column name}"}
            // 可选，默认导出所有列的所有版本
            // 注意：
            //   1.在多版本模式下，不支持常量列
            //   2.PK列不能指定，导出4元组中默认包括完整的PK
            //   3.不能重复指定列
            "column": [
                {"name":"attr1"}
            ],
            
            // 请求数据的Time Range,读取的范围是[begin,end)，左闭右开的区间
            // 可选，默认读取全部版本
            // 注意：begin必须小于end
            "timeRange":{
                // 可选，默认为0
                // 取值范围是0~LONG_MAX
                "begin":1400000000,
                // 可选，默认为Long Max(9223372036854775807L)
                // 取值范围是0~LONG_MAX
                "end"  :1600000000
            },
        
            // 请求的指定Version
            // 可选，默认读取所有版本
            // 取值范围是1~INT32_MAX
            "maxVersion":10,
          }
        },
        "writer": {
          //writer类型
          "name": "streamwriter",
          //是否打印内容
          "parameter": {
            "print": true
          }
        }
      }
    ]
  }
}
```
#### 3.1.3
* 配置一个从OTS **时序表**读取数据的reader(仅在newVersion == true时支持):
```json
{
  "job": {
    "setting": {
      "speed": {
        // 读取时序数据的通道数
        "channel": 5
      }
    },
    "content": [
      {
        "reader": {
          "name": "otsreader",
          "parameter": {
            "endpoint": "",
            "accessId": "",
            "accessKey": "",
            "instanceName": "",
            "table": "",
            // 读时序数据mode必须为normal
            "mode": "normal",
            // 读时序数据newVersion必须为true
            "newVersion": "true",
            // 配置该表为时序表
            "isTimeseriesTable":"true",
            // 配置需要读取时间线的measurementName字段，非必需
            // 为空则读取全表数据
            "measurementName":"measurement_5",
            // column是一个数组，每个元素表示一列
            // 对于常量列，需要配置以下字段:
            // 1. type : 字段值类型，必需
            //     支持类型 : string, int, double, bool, binary
            // 2. value : 字段值，必需
            // 
            // 对于普通列，需要配置以下字段:
            // 1. name : 列名,必需
            //     时间线的'度量名称'使用_m_name标识，数据类型为String
            //     时间线的'数据源'使用_data_source标识，数据类型为String
            //     时间线的'标签'使用_tags标识，数据类型为String
            //     时间线的'时间戳'使用_time标识，数据类型为Long
            // 2. is_timeseries_tag : 是否为tags字段内部的键值，非必需，默认为false。
            // 3. type : 字段值类型，非必需，默认为string。
            //     支持类型 : string, int, double, bool, binary
            "column": [
              {
                "name": "_m_name"
              },
              {
                "name": "tagA",
                "is_timeseries_tag":"true"
              },
              {
                "name": "double_0",
                "type":"DOUBLE"
              },
              {
                "name": "string_0",
                "type":"STRING"
              },
              {
                "name": "long_0",
                "type":"int"
              },
              {
                "name": "binary_0",
                "type":"BINARY"
              },
              {
                "name": "bool_0",
                "type":"BOOL"
              },
              {
                "type":"STRING",
                "value":"testString"
              }
            ]
          }
        },
        "writer": {
                    
                }
      }
    ]
  }
}

```

### 3.2 参数说明

* **endpoint**

    * 描述：OTS Server的EndPoint地址，例如http://bazhen.cn−hangzhou.ots.aliyuncs.com。

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

    * 描述：读取为多版本格式的数据，目前有两种模式。<br />
        * normal，对应普通的数据
        * multiVersion，写入数据为多版本格式的数据，多版本模式下，配置参数有所不同，详见3.1.2

    * 必选：否 <br />

    * 默认值：normal <br />

* **column**

    * 描述：所配置的表中需要同步的列名集合，使用JSON的数组描述字段信息。由于OTS本身是NoSQL系统，在OTSReader抽取数据过程中，必须指定相应地字段名称。

      支持普通的列读取，例如: {"name":"col1"}

      支持部分列读取，如用户不配置该列，则OTSReader不予读取。

      支持常量列读取，例如: {"type":"STRING", "value" : "DataX"}。使用type描述常量类型，目前支持STRING、INT、DOUBLE、BOOL、BINARY(用户使用Base64编码填写)、INF_MIN(OTS的系统限定最小值，使用该值用户不能填写value属性，否则报错)、INF_MAX(OTS的系统限定最大值，使用该值用户不能填写value属性，否则报错)。

      不支持函数或者自定义表达式，由于OTS本身不提供类似SQL的函数或者表达式功能，OTSReader也不能提供函数或表达式列功能。

    * 必选：是 <br />

    * 默认值：无 <br />

* **begin/end**

    * 描述：该配置项必须配对使用，用于支持OTS表范围抽取。begin/end中描述的是OTS **PrimaryKey**的区间分布状态，而且必须保证区间覆盖到所有的PrimaryKey，**需要指定该表下所有的PrimaryKey范围，不能遗漏任意一个PrimaryKey**，对于无限大小的区间，可以使用{"type":"INF_MIN"}，{"type":"INF_MAX"}指代。例如对一张主键为 [DeviceID, SellerID]的OTS进行抽取任务，begin/end可以配置为:

  ```json
      "range": {
          "begin": {
              {"type":"INF_MIN"},  //指定deviceID最小值
              {"type":"INT", "value":"0"}  //指定deviceID最小值
          },
          "end": {
              {"type":"INF_MAX"}, //指定deviceID抽取最大值
              {"type":"INT", "value":"9999"} //指定deviceID抽取最大值
          }
      }
  ```


	   如果要对上述表抽取全表，可以使用如下配置：

	```
		"range": {
			"begin": [
				{"type":"INF_MIN"},  //指定deviceID最小值
				{"type":"INF_MIN"} //指定SellerID最小值
			],
			"end": [
				{"type":"INF_MAX"}, //指定deviceID抽取最大值
		    	{"type":"INF_MAX"} //指定SellerID抽取最大值
			]
		}
	```

	* 必选：否 <br />

	* 默认值：读取全部值 <br />

* **split**

    * 描述：该配置项属于高级配置项，是用户自己定义切分配置信息，普通情况下不建议用户使用。适用场景通常在OTS数据存储发生热点，使用OTSReader自动切分的策略不能生效情况下，使用用户自定义的切分规则。split指定是的在Begin、End区间内的切分点，且只能是partitionKey的切分点信息，即在split仅配置partitionKey，而不需要指定全部的PrimaryKey。

      例如对一张主键为 [DeviceID, SellerID]的OTS进行抽取任务，可以配置为:

  ```json
      "range": {
          "begin": {
              {"type":"INF_MIN"},  //指定deviceID最小值
              {"type":"INF_MIN"}  //指定deviceID最小值
          },
          "end": {
              {"type":"INF_MAX"}, //指定deviceID抽取最大值
              {"type":"INF_MAX"} //指定deviceID抽取最大值
          }，
           // 用户指定的切分点，如果指定了切分点，Job将按照begin、end和split进行Task的切分，
          // 切分的列只能是Partition Key（ParimaryKey的第一列）
          // 支持INF_MIN, INF_MAX, STRING, INT
          "split":[
                              {"type":"STRING", "value":"1"},
                              {"type":"STRING", "value":"2"},
                              {"type":"STRING", "value":"3"},
                              {"type":"STRING", "value":"4"},
                              {"type":"STRING", "value":"5"}
                  ]
      }
  ```

    * 必选：否 <br />

    * 默认值：无 <br />


### 3.3 类型转换

目前OTSReader支持所有OTS类型，下面列出OTSReader针对OTS类型转换列表:


| DataX 内部类型| OTS 数据类型    |
| -------- | -----  |
| Long     |Integer |
| Double   |Double|
| String   |String|
| Boolean  |Boolean |
| Bytes    |Binary |


* 注意，OTS本身不支持日期型类型。应用层一般使用Long报错时间的Unix TimeStamp。


## 4 约束限制

### 4.1 一致性约束

OTS是类BigTable的存储系统，OTS本身能够保证单行写事务性，无法提供跨行级别的事务。对于OTSReader而言也无法提供全表的一致性视图。例如对于OTSReader在0点启动的数据同步任务，在整个表数据同步过程中，OTSReader同样会抽取到后续更新的数据，无法提供准确的0点时刻该表一致性视图。

### 4.2 增量数据同步

OTS本质上KV存储，目前只能针对PK进行范围查询，暂不支持按照字段范围抽取数据。因此只能对于增量查询，如果PK能够表示范围信息，例如自增ID，或者时间戳。

自增ID，OTSReader可以通过记录上次最大的ID信息，通过指定Range范围进行增量抽取。这样使用的前提是OTS中的PrimaryKey必须包含主键自增列(自增主键需要使用OTS应用方生成。)

时间戳，	OTSReader可以通过PK过滤时间戳，通过制定Range范围进行增量抽取。这样使用的前提是OTS中的PrimaryKey必须包含主键时间列(时间主键需要使用OTS应用方生成。)

## 5 FAQ
