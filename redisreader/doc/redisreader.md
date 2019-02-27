# DataX RedisReader 说明


------------

## 1 快速介绍

RedisReader 提供了读取Redis RDB 的能力。在底层实现上获取本地RDB文件/Redis Server数据，并转换为DataX传输协议传递给Writer。


## 2 功能与限制

1. 支持读取本地RDB/http RDB/redis server RDB的文件并转换成redis dump格式。

2. 支持过滤 DB/key名称过滤


我们暂时不能做到：

1. 单个RDB支持多线程并发读取。

2. Redis Server 未开启sync命令。

3. 读取rdb文件转换成json数据。


## 3 功能说明


### 3.1 配置样例

```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "redisreader",
                    "parameter": {
                        "connection": [
                            {
                                "uri": "file:///root/dump.rdb",
                                "uri": "http://localhost/dump.rdb",
                                "uri": "tcp://127.0.0.1:7001",
                                "uri": "tcp://127.0.0.1:7002",
                                "uri": "tcp://127.0.0.1:7003",

                            }
                        ],
                        "include":["^user"],
                        "exclude":["^password"],
                        "db":[0,1]
                    }
                },
                "writer": {
                    "name": "rediswriter",
                    "parameter": {
                        "connection": [
                            {
                                "uri": "tcp://127.0.0.1:6379",
                                "auth": "123456"
                            }
                        ],
                        "timeout":60000
                    }
                }
            }
        ],
        "setting": {
            "speed": {
                "channel": 1,
            }
        }
    },
    "core": {
        "transport": {
            "channel": {
                "byteCapacity": 524288000
            }
        }
    }
}
```

### 3.2 参数说明


* **redisreader.parameter.connection**

	* 描述：redis链接,支持多个本地rdb文件/http网络rdb文件/redis server rdb文件,如果是redis cluster 集群,请填写所有master节点地址<br />

	* 必选：是 <br />

	* 默认值：, <br />

* **redisreader.parameter.db**

	* 描述：过滤需要的db。 <br />

	* 必选：否 <br />

	* 默认值：所有db <br />

* **redisreader.parameter.include**

	* 描述：正则包含key。<br />

 	* 必选：否 <br />

 	* 默认值：<br />

* **redisreader.parameter.exclude**

	* 描述：正则排除key。<br />

 	* 必选：否 <br />

 	* 默认值： <br />

* **byteCapacity**

	* 描述：单个key过大可以做相应的调整默认64Mb。<br />

 	* 必选：否 <br />

 	* 默认值： <br />

* **channel**

	* 描述：通道数量 请填写 1。<br />

 	* 必选：否 <br />

 	* 默认值： <br />



## 4 性能报告


## 5 约束限制

1. 不支持直接读取任何不支持sync命令的redis server，如果需要请备份的rdb文件进行读取。

2. 如果是原生redis cluster集群，请填写所有master节点的tcp地址，redisreader插件会自动dump 所有节点的rdb文件。


## 6 FAQ

略


