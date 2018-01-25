# DataX ADS写入


---


## 1 快速介绍

<br />

欢迎ADS加入DataX生态圈！ADSWriter插件实现了其他数据源向ADS写入功能，现有DataX所有的数据源均可以无缝接入ADS，实现数据快速导入ADS。

ADS写入预计支持两种实现方式：

* ADSWriter 支持向ODPS中转落地导入ADS方式，优点在于当数据量较大时(>1KW)，可以以较快速度进行导入，缺点引入了ODPS作为落地中转，因此牵涉三方系统(DataX、ADS、ODPS)鉴权认证。

* ADSWriter 同时支持向ADS直接写入的方式，优点在于小批量数据写入能够较快完成(<1KW)，缺点在于大数据导入较慢。


注意：

> 如果从ODPS导入数据到ADS，请用户提前在源ODPS的Project中授权ADS Build账号具有读取你源表ODPS的权限，同时，ODPS源表创建人和ADS写入属于同一个阿里云账号。

-

> 如果从非ODPS导入数据到ADS，请用户提前在目的端ADS空间授权ADS Build账号具备Load data权限。

以上涉及ADS Build账号请联系ADS管理员提供。


## 2 实现原理

ADS写入预计支持两种实现方式：

### 2.1 Load模式

DataX 将数据导入ADS为当前导入任务分配的ADS项目表，随后DataX通知ADS完成数据加载。该类数据导入方式实际上是写ADS完成数据同步，由于ADS是分布式存储集群，因此该通道吞吐量较大，可以支持TB级别数据导入。

![中转导入](http://aligitlab.oss-cn-hangzhou-zmf.aliyuncs.com/uploads/cdp/cdp/f805dea46b/_____2015-04-10___12.06.21.png)

1. CDP底层得到明文的 jdbc://host:port/dbname + username + password + table， 以此连接ADS， 执行show grants; 前置检查该用户是否有ADS中目标表的Load Data或者更高的权限。注意，此时ADSWriter使用用户填写的ADS用户名+密码信息完成登录鉴权工作。

2. 检查通过后，通过ADS中目标表的元数据反向生成ODPS DDL，在ODPS中间project中，以ADSWriter的账户建立ODPS表（非分区表，生命周期设为1-2Day), 并调用ODPSWriter把数据源的数据写入该ODPS表中。
	
	注意，这里需要使用中转ODPS的账号AK向中转ODPS写入数据。
		
3. 写入完成后，以中转ODPS账号连接ADS，发起Load Data From ‘odps://中转project/中转table/' [overwrite] into adsdb.adstable [partition (xx,xx=xx)]; 这个命令返回一个Job ID需要记录。

	注意，此时ADS使用自己的Build账号访问中转ODPS，因此需要中转ODPS对这个Build账号提前开放读取权限。
	
4. 连接ADS一分钟一次轮询执行 select state from information_schema.job_instances where job_id like ‘$Job ID’，查询状态，注意这个第一个一分钟可能查不到状态记录。
	
5. Success或者Fail后返回给用户，然后删除中转ODPS表，任务结束。
	
上述流程是从其他非ODPS数据源导入ADS流程，对于ODPS导入ADS流程使用如下流程：

![直接导入](http://aligitlab.oss-cn-hangzhou-zmf.aliyuncs.com/uploads/cdp/cdp/b3a76459d1/_____2015-04-10___12.06.25.png)

### 2.2 Insert模式

DataX 将数据直连ADS接口，利用ADS暴露的INSERT接口直写到ADS。该类数据导入方式写入吞吐量较小，不适合大批量数据写入。有如下注意点：

* ADSWriter使用JDBC连接直连ADS，并只使用了JDBC Statement进行数据插入。ADS不支持PreparedStatement，故ADSWriter只能单行多线程进行写入。

* ADSWriter支持筛选部分列，列换序等功能，即用户可以填写列。

* 考虑到ADS负载问题，建议ADSWriter Insert模式建议用户使用TPS限流，最高在1W TPS。

* ADSWriter在所有Task完成写入任务后，Job Post单例执行flush工作，保证数据在ADS整体更新。


## 3 功能说明

### 3.1 配置样例

* 这里使用一份从内存产生到ADS，使用Load模式进行导入的数据。

```
{
  "job": {
    "setting": {
      "speed": {
        "channel": 2
      }
    },
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column": [
              {
                "value": "DataX",
                "type": "string"
              },
              {
                "value": "test",
                "type": "bytes"
              }
            ],
            "sliceRecordCount": 100000
          }
        },
        "writer": {
          "name": "adswriter",
          "parameter": {
            "odps": {
              "accessId": "xxx",
              "accessKey": "xxx",
              "account": "xxx@aliyun.com",
              "odpsServer": "xxx",
              "tunnelServer": "xxx",
              "accountType": "aliyun",
              "project": "transfer_project"
            },
            "writeMode": "load",
            "url": "127.0.0.1:3306",
            "schema": "schema",
            "table": "table",
            "username": "username",
            "password": "password",
            "partition": "",
            "lifeCycle": 2,
            "overWrite": true,
          }
        }
      }
    ]
  }
}
```

* 这里使用一份从内存产生到ADS，使用Insert模式进行导入的数据。

```
{
  "job": {
    "setting": {
      "speed": {
        "channel": 2
      }
    },
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column": [
              {
                "value": "DataX",
                "type": "string"
              },
              {
                "value": "test",
                "type": "bytes"
              }
            ],
            "sliceRecordCount": 100000
          }
        },
        "writer": {
          "name": "adswriter",
          "parameter": {
            "writeMode": "insert",
            "url": "127.0.0.1:3306",
            "schema": "schema",
            "table": "table",
            "column": ["*"],
            "username": "username",
            "password": "password",
            "partition": "id,ds=2015"
          }
        }
      }
    ]
  }
}
```



### 3.2 参数说明 （用户配置规格）

* **url**

	* 描述：ADS连接信息，格式为"ip:port"。
			                      
 	* 必选：是 <br />
 
	* 默认值：无 <br />
	
* **schema**

	* 描述：ADS的schema名称。
			                      
 	* 必选：是 <br />
 
	* 默认值：无 <br />
 
* **username**
 
	* 描述：ADS对应的username，目前就是accessId <br />
 
	* 必选：是 <br />
 
	* 默认值：无 <br />

* **password**

	* 描述：ADS对应的password，目前就是accessKey <br />
 
	* 必选：是 <br />
 
	* 默认值：无 <br />

* **table**

	* 描述：目的表的表名称。
      	 
	* 必选：是 <br />
 
	* 默认值：无 <br />
	

* **partition**

	* 描述：目标表的分区名称，当目标表为分区表，需要指定该字段。
      	 
	* 必选：否 <br />
 
	* 默认值：无 <br />
	
* **writeMode**

	* 描述：支持Load和Insert两种写入模式
	
 	* 必选：是 <br />  
 	
 	* 默认值：无 <br />  
	
* **column**

	* 描述：目的表字段列表，可以为["*"]，或者具体的字段列表，例如["a", "b", "c"]
      	 
	* 必选：是 <br />
 
	* 默认值：无 <br />
	
* **overWrite**

	* 描述：ADS写入是否覆盖当前写入的表，true为覆盖写入，false为不覆盖(追加)写入。当writeMode为Load，该值才会生效。
	
 	* 必选：是 <br />  
 	
 	* 默认值：无 <br />  


* **lifeCycle**

	* 描述：ADS 临时表生命周期。当writeMode为Load时，该值才会生效。
	
 	* 必选：是 <br />  
 	
 	* 默认值：无 <br /> 
 
 * **batchSize**

	* 描述：ADS 提交数据写的批量条数，当writeMode为insert时，该值才会生效。
	
 	* 必选：writeMode为insert时才有用 <br />  
 	
 	* 默认值：32 <br /> 

* **bufferSize**

	* 描述：DataX数据收集缓冲区大小，缓冲区的目的是攒一个较大的buffer，源头的数据首先进入到此buffer中进行排序，排序完成后再提交ads写。排序是根据ads的分区列模式进行的，排序的目的是数据顺序对ADS服务端更友好，出于性能考虑。bufferSize缓冲区中的数据会经过batchSize批量提交到ADS中，一般如果要设置bufferSize，设置bufferSize为batchSize数量的多倍。当writeMode为insert时，该值才会生效。
	
 	* 必选：writeMode为insert时才有用 <br />  
 	
 	* 默认值：默认不配置不开启此功能 <br /> 
 

### 3.3 类型转换

| DataX 内部类型| ADS 数据类型    |
| -------- | -----  |
| Long     |int, tinyint, smallint, int, bigint|
| Double   |float, double, decimal|
| String   |varchar    | 
| Date     |date    | 
| Boolean  |bool   |  
| Bytes    |无   | 
 
 注意:

* multivalue  ADS支持multivalue类型，DataX对于该类型支持待定？
 

##  4  插件约束

如果Reader为ODPS，且ADSWriter写入模式为Load模式时，ODPS的partition只支持如下三种配置方式(以两级分区为例)：
```
"partition":["pt=*,ds=*"]  (读取test表所有分区的数据)
"partition":["pt=1,ds=*"]  (读取test表下面，一级分区pt=1下面的所有二级分区)
"partition":["pt=1,ds=hangzhou"] (读取test表下面，一级分区pt=1下面，二级分区ds=hz的数据)
```

## 5 性能报告（线上环境实测）

### 5.1 环境准备

### 5.2 测试报告

## 6 FAQ
