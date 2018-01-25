# DataX FtpWriter 说明


------------

## 1 快速介绍

FtpWriter提供了向远程FTP文件写入CSV格式的一个或者多个文件，在底层实现上，FtpWriter将DataX传输协议下的数据转换为csv格式，并使用FTP相关的网络协议写出到远程FTP服务器。

**写入FTP文件内容存放的是一张逻辑意义上的二维表，例如CSV格式的文本信息。**


## 2 功能与限制

FtpWriter实现了从DataX协议转为FTP文件功能，FTP文件本身是无结构化数据存储，FtpWriter如下几个方面约定:

1. 支持且仅支持写入文本类型(不支持BLOB如视频数据)的文件，且要求文本中shema为一张二维表。

2. 支持类CSV格式文件，自定义分隔符。

3. 写出时不支持文本压缩。

6. 支持多线程写入，每个线程写入不同子文件。

我们不能做到：

1. 单个文件不能支持并发写入。


## 3 功能说明


### 3.1 配置样例


```json
{
    "setting": {},
    "job": {
        "setting": {
            "speed": {
                "channel": 2
            }
        },
        "content": [
            {
                "reader": {},
                "writer": {
                    "name": "ftpwriter",
                    "parameter": {
                        "protocol": "sftp",
                        "host": "***",
                        "port": 22,
                        "username": "xxx",
                        "password": "xxx",
                        "timeout": "60000",
                        "connectPattern": "PASV",
                        "path": "/tmp/data/",
                        "fileName": "yixiao",
                        "writeMode": "truncate|append|nonConflict",
                        "fieldDelimiter": ",",
                        "encoding": "UTF-8",
                        "nullFormat": "null",
                        "dateFormat": "yyyy-MM-dd",
                        "fileFormat": "csv",
                        "header": []
                    }
                }
            }
        ]
    }
}
```

### 3.2 参数说明

* **protocol**

	* 描述：ftp服务器协议，目前支持传输协议有ftp和sftp。 <br />
 
	* 必选：是 <br />
 
	* 默认值：无 <br />
	
* **host**

	* 描述：ftp服务器地址。 <br />
 		 
	* 必选：是 <br />
 
	* 默认值：无 <br />
	
* **port**

	* 描述：ftp服务器端口。 <br />
 
	* 必选：否 <br />
 
	* 默认值：若传输协议是sftp协议，默认值是22；若传输协议是标准ftp协议，默认值是21 <br />
	
* **timeout**

	* 描述：连接ftp服务器连接超时时间，单位毫秒。 <br />
 
	* 必选：否 <br />
 
	* 默认值：60000（1分钟）<br />		
	
* **username**

	* 描述：ftp服务器访问用户名。 <br />
 
	* 必选：是 <br />
 
	* 默认值：无 <br />

* **password**

	* 描述：ftp服务器访问密码。 <br />
 
	* 必选：是 <br />
 
	* 默认值：无 <br />

* **path**

	* 描述：FTP文件系统的路径信息，FtpWriter会写入Path目录下属多个文件。 <br />
			 
	* 必选：是 <br />
 
	* 默认值：无 <br />
 
* **fileName**
 
 	* 描述：FtpWriter写入的文件名，该文件名会添加随机的后缀作为每个线程写入实际文件名。 <br />
			 
	* 必选：是 <br />
 
	* 默认值：无 <br />

* **writeMode**
 
 	* 描述：FtpWriter写入前数据清理处理模式： <br />
			 
		* truncate，写入前清理目录下一fileName前缀的所有文件。
		* append，写入前不做任何处理，DataX FtpWriter直接使用filename写入，并保证文件名不冲突。
		* nonConflict，如果目录下有fileName前缀的文件，直接报错。
		
	* 必选：是 <br />
 
	* 默认值：无 <br />

* **fieldDelimiter**

	* 描述：读取的字段分隔符 <br />
 
	* 必选：否 <br />
 
	* 默认值：, <br />

* **compress**

	* 描述：文本压缩类型，暂时不支持。 <br />
 
	* 必选：否 <br />
 
	* 默认值：无压缩 <br />
 	
* **encoding**

	* 描述：读取文件的编码配置。<br />
 
 	* 必选：否 <br />
 
 	* 默认值：utf-8 <br />
 

* **nullFormat**

	* 描述：文本文件中无法使用标准字符串定义null(空指针)，DataX提供nullFormat定义哪些字符串可以表示为null。<br />
 
		 例如如果用户配置: nullFormat="\N"，那么如果源头数据是"\N"，DataX视作null字段。

 	* 必选：否 <br />
 
 	* 默认值：\N <br />

* **dateFormat**

	* 描述：日期类型的数据序列化到文件中时的格式，例如 "dateFormat": "yyyy-MM-dd"。<br />
 
 	* 必选：否 <br />
 
 	* 默认值：无 <br />

* **fileFormat**

	* 描述：文件写出的格式，包括csv (http://zh.wikipedia.org/wiki/%E9%80%97%E5%8F%B7%E5%88%86%E9%9A%94%E5%80%BC) 和text两种，csv是严格的csv格式，如果待写数据包括列分隔符，则会按照csv的转义语法转义，转义符号为双引号"；text格式是用列分隔符简单分割待写数据，对于待写数据包括列分隔符情况下不做转义。<br />
 
 	* 必选：否 <br />
 
 	* 默认值：text <br />

* **header**

	* 描述：txt写出时的表头，示例['id', 'name', 'age']。<br />
 
 	* 必选：否 <br />
 
 	* 默认值：无 <br />

### 3.3 类型转换


FTP文件本身不提供数据类型，该类型是DataX FtpWriter定义：

| DataX 内部类型| FTP文件 数据类型    |
| -------- | -----  |
| 
| Long     |Long -> 字符串序列化表示|
| Double   |Double -> 字符串序列化表示|
| String   |String -> 字符串序列化表示| 
| Boolean  |Boolean -> 字符串序列化表示| 
| Date     |Date -> 字符串序列化表示|

其中：

* FTP文件 Long是指FTP文件文本中使用整形的字符串表示形式，例如"19901219"。
* FTP文件 Double是指FTP文件文本中使用Double的字符串表示形式，例如"3.1415"。
* FTP文件 Boolean是指FTP文件文本中使用Boolean的字符串表示形式，例如"true"、"false"。不区分大小写。
* FTP文件 Date是指FTP文件文本中使用Date的字符串表示形式，例如"2014-12-31"，Date可以指定format格式。


## 4 性能报告


## 5 约束限制

略

## 6 FAQ

略

