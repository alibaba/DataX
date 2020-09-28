大数据平台通过将所有数据整合起来，充分分析与挖掘数据的内在价值，为业务部门提供数据平台，数据产品与数据服务。


大数据平台接入的数据中可能包括很多用户的隐私和敏感信息，如用户在酒店的入住纪录，用户支付信息等，这些数据存在可能泄漏的风险。大数据平台一般通过用户认证，权限管理以及数据加密等技术保证数据的安全，但是这并不能完全从技术上保证数据的安全。严格的来说，任何有权限访问用户数据的人员，如ETL工程师或是数据分析人员等，均有可能导致数据泄漏的风险。另一方面，没有访问用户数据权限的人员，也可能有对该数据进行分析挖掘的需求，数据的访问约束大大限制的充分挖掘数据价值的范围。

数据脱敏通过对数据进行脱敏，在保证数据可用性的同时，也在一定范围内保证恶意攻击者无法将数据与具体用户关联到一起，从而保证用户数据的隐私性。数据脱敏方案作为大数据平台整体数据安全解决方案的重要组成部分，是构建安全可靠的大数据平台必不可少的功能特性。

## DataX-Masking 简介
[DataX-Masking](https://github.com/ECNU-1X/DataX-Masking) 是在 [DataX 3.0](https://github.com/alibaba/DataX/) 框架基础上二次开发得到的**大数据脱敏平台**，可以快速地在数据传输过程中对指定的单个或多个字段用可选的脱敏方法进行处理，为数据脱敏方案的实现提供支持。

## DataX 基本功能的部署与使用
请参考 [DataX Quick Start](userGuid.md)

成功部署后，就要编写job配置文件了，按需求配置好:
* reader
* writer
* *transformer

三个部分(其中transformer是平台发挥脱敏功能的部分，如果不需要脱敏可以省略), 然后只需打开datax程序文件目录, 找到bin文件夹中的datax.py,执行以下命令：
> python datax.py path/to/job.json

然后整个作业就会按job文件指定的需求运行了！

基本的job配置方法可以参考[MySQL Reader](https://github.com/alibaba/DataX/blob/master/mysqlreader/doc/mysqlreader.md)和[MySQL Writer](https://github.com/alibaba/DataX/blob/master/mysqlwriter/doc/mysqlwriter.md)。

如何添加脱敏操作，请参考以下资料，按需编写transformer的job配置文件吧。

## 脱敏方法功能说明
平台中的脱敏方法可以分为两类，一种是常用的脱敏方法，这种方法计算开销比较小；另一种是加密方法，这种方法计算开销较大，一般而言用时较久。

|脱敏方法名称|描述|示例|
|---|---|---|
|Hiding|将数据置为常量，一般用于处理不需要的敏感字段。|500 ->0<br>false->true|
|Floor|对整数或浮点数或者日期向下取整。|-12.68->-12<br>12580->12000<br>2018-05-10 10:17->2018-05-01 6:00|
|Enumerate|将数字映射为新值，同时保持数据的大小顺序。|500->1500 600->1860 700->2000|
|Prefix Preserve|保持前n位不变，混淆其余部分。可针对字母和数字字符在同为字母或数字范围内进行混淆，特殊符号将保留。|10.199.90.105->10.199.38.154<br>18965432100->18985214789|
|MD5|不可逆的hash摘要方法。将不定长的数据映射成定长的数据(长度为32的字符串)。|你好世界！->4f025928d787aa7b73beb58c1a85b11d|
|EDP|Epsilon Differential Privacy | 17.5 -> 17.962 |
|AES|AES-128-CBC 对称加密|你好世界！-> 12da3fedd5f0992447b1c7b4af0d7133|
| FPE | format Preserving Encryption | abcdefg -> iskejtl |
| RSA | RSA 非对称密钥加密算法 | 加密：明文->长度为256字串(1024位二进制整数的16进制表示法)<br>解密：加密后的字串->明文 | 

**当前版本保型加密字符串中只能包含小写字母而不能有任何其他字符**


## 脱敏方法数据类型支持情况
| DataX 内部类型|  Floor    | Hiding | Enumerate|Prefix-Preserve|MD5|EDP|RSA|AES|FPE|
| -------- | :-:  | :-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| Long     |√|√| √|√||√|||
| Double   |√|√||||√|||
| String   |   |√||√|√||√|√|√|
| Date     |√|√||||||
| Boolean  |    |√||||||
| Bytes    |     |||||||
| 可逆 |||||||:coffee:||:construction:|
|UTF-8编码下多语言支持||√|||√|:construction:||:construction:|

当前版本RSA解密尚不稳定。

## Masking Transformer 配置编写介绍
* columnIndex：columnIndex
* name：transformer的代号，用于选择不同的脱敏方法。

想使用好datax-masking平台，最重要的工作就是学习如何编写job配置文件了！

1. Hiding
将数据置零，一般用于处理不需要的敏感字段。

```
paras:1个，可置空。
{
    "name": "dx_hiding",
    "parameter": 
        {
            "columnIndex":3,
            "paras":[""]
        }  
}
```

2. Floor
对整数或浮点数或者日期向下取整。
job.json 编写示例：

```
对日期数据
paras:可以选择取整维度,命令中包含Y则在年份维度按年代向下取整，包含M则调整月份为一月，包含D把日期调为本月第一天，包含H则对小时进行取整，包含m调整分钟0，包含s调整。
{
    "name": "dx_floor",
    "parameter": 
        {
            "columnIndex":3,
            "paras":["YMDHms"]
        }  
}
```

```
对整数
paras:可以提供 mod 用于选择不同程度的取整，即除以 mod 后取商再乘 mod
{
    "name": "dx_floor",
    "parameter": 
        {
            "columnIndex":0,
            "paras":["mod"]
        }  
}
```



3. Enumerate
将数字映射为新值，同时保持数据的大小顺序。

```
paras:1个，正整数，整数越大则数字之间的差距有较大概率放大。如果参数为0，则保持原数据不变。
{
    "name": "dx_enum",
    "parameter": 
        {
            "columnIndex":3,
            "paras":["100"]
        }  
}
```

4. Prefix Preserve
保持前n位不变，混淆其余部分。可针对字母和数字字符在同为字母或数字范围内进行混淆，特殊符号将保留。
```
paras:1个，正整数型字符串，表示要保留的位数。
{
    "name": "dx_prefix_preserve",
    "parameter": 
        {
            "columnIndex":3,
            "paras":["6"]
        }  
}
```

5. MD5
不可逆的hash摘要方法。将不定长的数据映射成定长的数据(长度为32的字符串)。

```
paras:0个，建议置空
{
    "name": "dx_md5",
    "parameter": 
        {
            "columnIndex":3,
            "paras":[""]
        }  
}
```

6. 加密
采用密码学方法对数据进行加密，加密的计算开销较为明显。

| 加密算法代号 | 加密算法 | 支持数据类型 | 参数说明 |
| - | :-: | :-: | - |
| AES | AES-128-CBC | String | 加密算法不需要额外参数 |
| FPE | format Preserving Encryption | String | 不需要额外参数，**字符串中只能包含小写字母而不能有任何其他字符** |
| RSA | RSA 非对称密钥加密算法 | String | 缺省情况下私钥和密钥由系统生成并以.pem形式存储在本地。参数：<br>*private_encrypt* 私钥加密；*private_decrypt* 私钥解密；*public_encrypt* 公钥加密；*public_decrypt* 公钥解密 |

第一个参数指明需要采用的密码学算法（算法代号）。


6.1 AES加密
```
paras:有效为1个，第二个参数置空

{
    "name": "dx_cryp",
    "parameter": 
        {
        "columnIndex":1,
        "paras":["AES", ""]
        }  
}
```
6.2 保型加密
```
paras:有效参数1个，第二个参数置空

{
    "name": "dx_cryp",
    "parameter": 
        {
        "columnIndex":1,
        "paras":["FPE",""]
        }  
}
```

6.3 RSA加密/解密

paras 第二个参数：*private_encrypt* 私钥加密；*private_decrypt* 私钥解密；*public_encrypt* 公钥加密；*public_decrypt* 公钥解密

```
RSA私钥解密
paras 有效参数2个，第二个参数指明具体使用的方法。
Transformer配置示例：

{
    "name": "dx_cryp",
    "parameter": 
        {
        "columnIndex":2,
        "paras":["RSA", "private_decrypt"]
        }  
}

表示对第2列（下标从0开始）数据字段采用私钥解密的数据转换方法。
```
## 当前版本 0.1 Alpha

### Notice!
* 当前版本仅在开发环境下进行过测试
* 当前版本RSA解密仍不稳定，建议暂勿使用
* 当前版本每次仅可使用一种密码学方法

### Future
* 稳定的RSA加密解密
* 提供可对中文字符进行加密的保型加密
* 提供动态加载自定义transformer的功能

## 附录

### 1. 用于在本地测试不同脱敏方法的job文件
```
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            },
            "errorLimit": {
                "record": 0
            }
        },
        "content": [
            {
                "reader": {
                    "name": "streamreader",
                    "parameter": {
                        "column": [
                            {
                                "value": "192.168.1.109",
                                "type": "string"
                            },
                            {
                                "value": 100.5,
                                "type": "double"
                            },
                            {
                                "value": 521,
                                "type": "long"
                            },
                            {
                                "value": "2018-05-08 12:12:24",
                                "type": "date"
                            },
                            {
                                "value": false,
                                "type": "bool"
                            },
                            {
                                "value": "中文test",
                                "type": "string"
                            },
                            {
                                "value": "中文test",
                                "type": "string"
                            }
                        ],
                        "sliceRecordCount": 100
                    }
                },
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print": true,
                        "encoding": "UTF-8"
                    }
                },
                "transformer": [
                    {
                        "name": "dx_prefix_preserve",
                        "parameter": 
                            {
                                "columnIndex":0,
                                "paras":["7"]
                            }  
                    },
                    {
                        "name": "dx_edp",
                        "parameter": 
                            {
                                "columnIndex":1,
                                "paras":["5"]
                            }  
                    },
                    {
                        "name": "dx_enum",
                        "parameter": 
                            {
                                "columnIndex":2,
                                "paras":["100"]
                            }  
                    },
                    {
                        "name": "dx_floor",
                        "parameter": 
                            {
                                "columnIndex":3,
                                "paras":["YMDHms"]
                            }  
                    },
                    {
                        "name": "dx_hiding",
                        "parameter": 
                            {
                                "columnIndex":4,
                                "paras":[""]
                            }  
                    },
                    {
                        "name": "dx_md5",
                        "parameter": 
                            {
                                "columnIndex":5,
                                "paras":[""]
                            }  
                    },
                    {
                        "name": "dx_hiding",
                        "parameter": 
                            {
                                "columnIndex":6,
                                "paras":[""]
                            }  
                    }
                ]
            }
        ]
    }
}

```

### RSA加密解密操作补充说明
支持全部四种加密解密操作：
1. 公钥加密；
2. 公钥解密；
3. 私钥加密；
4. 私钥解密。

公钥加密后配合私钥解密可以实现针对特定对象的加密传输，可以用来交易数据。

私钥加密后配合公钥解密可以实现签名功能，用以验证数据来源。

**为了保证加密解密过程的有效性，这里在加密过程中取消了padding方法，这使得对于需要加密的明文，其对应的bite位长度应小于秘钥长度(1024）位，**
点此查看[原因](http://arganzheng.life/input-too-large-for-RSA-cipher.html)。

公钥和私钥的配置：bin 目录下，如果用户不提供公钥(id_rsa.pub))和私钥文件(id_rsa)，系统将自动生成秘钥对并存储于本地，秘钥内容格式为pem。

Ps: 我们在ubuntu 16.04系统上结合MySQL5.7进行了一百万条简单的字符串加密解密实验，用公钥加密后，使用私钥解密，最终实验中有若干条在私钥解密阶段出错并被作为脏数据丢弃，错误暂无法稳定重现，目前正在解决