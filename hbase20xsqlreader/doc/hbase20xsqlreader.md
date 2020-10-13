# hbase20xsqlreader  插件文档


___


 
## 1 快速介绍

hbase20xsqlreader插件实现了从Phoenix(HBase SQL)读取数据，对应版本为HBase2.X和Phoenix5.X。

## 2 实现原理

简而言之，hbase20xsqlreader通过Phoenix轻客户端去连接Phoenix QueryServer，并根据用户配置信息生成查询SELECT 语句，然后发送到QueryServer读取HBase数据，并将返回结果使用DataX自定义的数据类型拼装为抽象的数据集，最终传递给下游Writer处理。

## 3 功能说明

### 3.1 配置样例

* 配置一个从Phoenix同步抽取数据到本地的作业:

```
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "hbase20xsqlreader",  //指定插件为hbase20xsqlreader
                    "parameter": {
                        "queryServerAddress": "http://127.0.0.1:8765",  //填写连接Phoenix QueryServer地址
                        "serialization": "PROTOBUF",  //QueryServer序列化格式
                        "table": "TEST",    //读取表名
                        "column": ["ID", "NAME"],   //所要读取列名
                        "splitKey": "ID"    //切分列，必须是表主键
                    }
                },
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "encoding": "UTF-8",
                        "print": true
                    }
                }
            }
        ],
        "setting": {
            "speed": {
                "channel": "3"
            }
        }
    }
}
```


### 3.2 参数说明

* **queryServerAddress**

	* 描述：hbase20xsqlreader需要通过Phoenix轻客户端去连接Phoenix QueryServer，因此这里需要填写对应QueryServer地址。
           增强版/Lindorm 用户若需透传user, password参数，可以在queryServerAddress后增加对应可选属性.
           格式参考：http://127.0.0.1:8765;user=root;password=root
            
	* 必选：是 <br />
 
	* 默认值：无 <br />
 
* **serialization**
 
	* 描述：QueryServer使用的序列化协议
 
	* 必选：否 <br />
 
	* 默认值：PROTOBUF <br />
	
* **table**

	* 描述：所要读取表名
	
	* 必选：是 <br />
 
	* 默认值：无 <br />
	
* **schema**

	* 描述：表所在的schema
	
	* 必选：否 <br />
 
	* 默认值：无 <br />
		
* **column**

	* 描述：填写需要从phoenix表中读取的列名集合，使用JSON的数组描述字段信息，空值表示读取所有列。
 
	* 必选： 否<br />
 
	* 默认值：全部列 <br />
 
* **splitKey**

	* 描述：读取表时对表进行切分并行读取，切分时有两种方式：1.根据该列的最大最小值按照指定channel个数均分，这种方式仅支持整形和字符串类型切分列；2.根据设置的splitPoint进行切分
	
	* 必选：是 <br />
 
	* 默认值：无 <br />

* **splitPoints**

	* 描述：由于根据切分列最大最小值切分时不能保证避免数据热点，splitKey支持用户根据数据特征动态指定切分点，对表数据进行切分。建议切分点根据Region的startkey和endkey设置，保证每个查询对应单个Region
 
	* 必选： 否<br />
 
	* 默认值：无 <br />
	
* **where**
    
    * 描述：支持对表查询增加过滤条件，每个切分都会携带该过滤条件。
     
    * 必选： 否<br />
     
    * 默认值：无<br />
    
* **querySql**
        
    * 描述：支持指定多个查询语句，但查询列类型和数目必须保持一致，用户可根据实际情况手动输入表查询语句或多表联合查询语句，设置该参数后，除queryserverAddress参数必须设置外，其余参数将失去作用或可不设置。
         
    * 必选： 否<br />
         
    * 默认值：无<br />
    

### 3.3 类型转换

目前hbase20xsqlreader支持大部分Phoenix类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出MysqlReader针对Mysql类型转换列表:


| DataX 内部类型| Phoenix 数据类型    |
| -------- | -----  |
| String     |CHAR, VARCHAR|
| Bytes   |BINARY, VARBINARY|
| Bool   |BOOLEAN   | 
| Long     |INTEGER, TINYINT, SMALLINT, BIGINT  | 
| Double  |FLOAT, DECIMAL, DOUBLE,   |  
| Date    |DATE, TIME, TIMESTAMP    | 



## 4 性能报告

略

## 5 约束限制

* 切分表时切分列仅支持单个列，且该列必须是表主键
* 不设置splitPoint默认使用自动切分，此时切分列仅支持整形和字符型
* 表名和SCHEMA名及列名大小写敏感，请与Phoenix表实际大小写保持一致
* 仅支持通过Phoenix QeuryServer读取数据，因此您的Phoenix必须启动QueryServer服务才能使用本插件

## 6 FAQ

***


