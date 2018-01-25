# DataX OCSWriter 适用memcached客户端写入ocs
---
## 1 快速介绍
### 1.1 OCS简介
开放缓存服务( Open Cache Service，简称OCS）是基于内存的缓存服务，支持海量小数据的高速访问。OCS可以极大缓解对后端存储的压力，提高网站或应用的响应速度。OCS支持Key-Value的数据结构，兼容Memcached协议的客户端都可与OCS通信。</br>

OCS 支持即开即用的方式快速部署；对于动态Web、APP应用，可通过缓存服务减轻对数据库的压力，从而提高网站整体的响应速度。</br>

与本地MemCache相同之处在于OCS兼容Memcached协议，与用户环境兼容，可直接用于OCS服务 不同之处在于硬件和数据部署在云端，有完善的基础设施、网络安全保障、系统维护服务。所有的这些服务，都不需要投资，只需根据使用量进行付费即可。
### 1.2 OCSWriter简介
OCSWriter是DataX实现的，基于Memcached协议的数据写入OCS通道。
## 2 功能说明
### 2.1 配置样例
* 这里使用一份从内存产生的数据导入到OCS。

```
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
                        "column": [
                            {
                                "value": "DataX",
                                "type": "string"
                            },
                            {
                                "value": 19880808,
                                "type": "long"
                            },
                            {
                                "value": "1988-08-08 08:08:08",
                                "type": "date"
                            },
                            {
                                "value": true,
                                "type": "bool"
                            },
                            {
                                "value": "test",
                                "type": "bytes"
                            }
                        ],
                        "sliceRecordCount": 1000
                    }
                },
                "writer": {
                    "name": "ocswriter",
                    "parameter": {
                        "proxy": "xxxx",
                        "port": "11211",
                        "userName": "user",
                        "password": "******",
                        "writeMode": "set|add|replace|append|prepend",
                        "writeFormat": "text|binary",
                        "fieldDelimiter": "\u0001",
                        "expireTime": 1000,
                        "indexes": "0,2",
                        "batchSize": 1000
                    }
                }
            }
        ]
    }
}
```

### 2.2 参数说明

* **proxy**

    * 描述：OCS机器的ip或host。
    * 必选：是

* **port**

	* 描述：OCS的连接域名，默认为11211
	* 必选：否
	* 默认值：11211

* **username**

	* 描述：OCS连接的访问账号。
	* 必选：是

* **password**

	* 描述：OCS连接的访问密码
	* 必选：是

* **writeMode**

	* 描述: OCSWriter写入方式，具体为：
		* set: 存储这个数据，如果已经存在则覆盖
		* add: 存储这个数据，当且仅当这个key不存在的时候
		* replace: 存储这个数据，当且仅当这个key存在
		* append: 将数据存放在已存在的key对应的内容的后面，忽略exptime
		* prepend: 将数据存放在已存在的key对应的内容的前面，忽略exptime
	* 必选：是

* **writeFormat**

	* 描述: OCSWriter写出数据格式，目前支持两类数据写入方式:
		* text: 将源端数据序列化为文本格式，其中第一个字段作为OCS写入的KEY，后续所有字段序列化为STRING类型，使用用户指定的fieldDelimiter作为间隔符，将文本拼接为完整的字符串再写入OCS。
		* binary: 将源端数据作为二进制直接写入，这类场景为未来做扩展使用，目前不支持。如果填写binary将会报错！
	* 必选：否
	* 默认值：text

* **expireTime**

	* 描述: OCS值缓存失效时间，目前MemCache支持两类过期时间，

		* Unix时间(自1970.1.1开始到现在的秒数)，该时间指定了到未来某个时刻数据失效。
		* 相对当前时间的秒数，该时间指定了从现在开始多长时间后数据失效。
		**注意：如果过期时间的秒数大于60*60*24*30(即30天)，则服务端认为是Unix时间。**
        * 单位：秒
	* 必选：否
	* 默认值：0【0表示永久有效】

* **indexes**

	* 描述: 用数据的第几列当做ocs的key
	* 必选：否
	* 默认值：0

* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与OCS的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况[memcached版本暂不支持批量写]。
	* 必选：否
	* 默认值：256

* **fieldDelimiter**
    * 描述：写入ocs的key和value分隔符。比如：key=tom\u0001boston, value=28\u0001lawer\u0001male\u0001married
    * 必选：否
    * 默认值：\u0001

## 3 性能报告
### 3.1 datax机器配置
```
CPU:16核、内存:24GB、网卡:单网卡1000mbps
```
### 3.2 任务资源配置
```
-Xms8g -Xmx8g -XX:+HeapDumpOnOutOfMemoryError
```
### 3.3 测试报告
| 单条数据大小      |    通道并发数 | TPS  |  通道流量  |  出口流量  |  备注  |
| :--------: | :--------:| :--: | :--: | :--: | :--: |
| 1KB  | 1 |  579 tps  |  583.31KB/s  |  648.63KB/s  |  无  |
| 1KB  | 10 |  6006 tps  |  5.87MB/s  |  6.73MB/s  |  无  |
| 1KB  | 100 |  49916 tps  |  48.56MB/s  |  55.55MB/s  |  无  |
| 10KB  | 1 | 438 tps  |  4.62MB/s  |  5.07MB/s  |  无  |
| 10KB  | 10 | 4313 tps  |  45.57MB/s  |  49.51MB/s  |  无  |
| 10KB  | 100 | 10713 tps  |  112.80MB/s  |  123.01MB/s  |  无  |
| 100KB  | 1 | 275 tps  |  26.09MB/s  |  144.90KB/s  |  无。数据冗余大，压缩比高。  |
| 100KB  | 10 | 2492 tps  |  236.33MB/s  |  1.30MB/s  |  无  |
| 100KB  | 100 | 3187 tps  |  302.17MB/s  | 1.77MB/s  |  无  |

### 3.4 性能测试小结
1. 单条数据小于10KB时建议开启100并发。
2. 不建议10KB以上的数据写入ocs。
