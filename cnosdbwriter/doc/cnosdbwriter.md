## 1 快速介绍

用于将数据导入 CnosDB。

## 2 实现原理

通过 DataX 框架接收 reader 插件提供的数据，过滤出在配置文件中指定的列，转换为 CnosDB 的无模式写入语句，通过 [HTTP REST API](https://docs.cnosdb.com/zh/latest/reference/rest_api.html#接口列表) 发送至 CnosDB。

## 3 配置说明

### 3.1 配置样例

假设有 MySQL 表如下：

```sql
use test_db;

create table test_1
(
    id   bigint auto_increment primary key,
    time datetime null,
    host varchar(10) null comment 'The host tag column',
    unit varchar(20) null comment 'The unit tag column',
    fa   int null,
    fb   bigint null,
    fc   double null,
    fd   text null,
    fe   bit null
);
```

可编写以下配置将其导入到 CnosDB 中。

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "username": "root",
            "password": "root",
            "connection": [
              {
                "querySql": [
                  "select time, host, unit, fa, fb, fc, fd, fe from test_1;"
                ],
                "jdbcUrl": [
                  "jdbc:mysql://127.0.0.1:3306/test_db"
                ]
              }
            ]
          }
        },
        "writer": {
          "name": "cnosdbwriter",
          "parameter": {
            "cnosdbWriteAPI": "http://127.0.0.1:8902/api/v1/write",
            "tenant": "cnosdb",
            "database": "public",
            "username": "root",
            "password": "root",
            "batchSize": 1000,
            "format": "datax",
            "table": "test_1",
            "tags": {
              "host": 1,
              "unit": 2
            },
            "fields": {
              "fa": 3,
              "fb": 4,
              "fc": 5,
              "fd": 6,
              "fe": 7
            },
            "timeIndex": 0,
            "precision": "ms"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 1
      }
    }
  }
}
```

### 3.2 参数说明

* `cnosdbWriteAPI`
    * 描述：CnosDB 写 API 的 URL，字符串
    * 必选：否
    * 默认值：`http://127.0.0.1:8902/api/v1/write`

* `tenant`
    * 描述：租户，字符串
    * 必选：否
    * 默认值：`cnosdb`

* `database`
    * 描述：数据库，字符串
    * 必选：否
    * 默认值：`public`

* `username`
    * 描述：用户名，字符串
    * 必选：否
    * 默认值：`root`

* `password`
    * 描述：密码，字符串
    * 必选：否
    * 默认值：`root`

* `batchSize`
    * 描述：每批次写入 CnosDB 的最大行数，无符号整数
    * 必选：否
    * 默认值：1000

* `bufferSize`
    * 描述：每批次写入 CnosDB 的最大字节数，无符号整数
    * 必选：否
    * 默认值：1024 * 1024 * 8
    * 示例：`1048576`

* `format`
    * 描述：reader 所使用的格式，字符串。如果使用了特殊格式（如 opentsdbreader），那么需要提供该配置，以提示 cnosdbwriter
      进行特殊处理
    * 必选：否
    * 默认值：`datax`
    * 可选值：`datax`, `opentsdb`
    * 备注：在使用 `opentsdb` 时，不需要额外设置 `table`, `tags`, `fields`, `timeIndex`

* `table`
    * 描述：表，字符串
    * 必选：format 为 datax 时必选
    * 默认值：无
    * 示例：`table_cpu_usages`

* `tags`
    * 描述：Map 类型，Tag 名称与对应输入列的序号（无符号整数）的映射
    * 必选：format 为 datax 时必选
    * 默认值：无
    * 示例：`{ "host": 1, "core": 3 }`

* `fields`
    * 描述：Map 类型，Field 名称与对应输入列的序号（无符号整数）的映射
    * 必选：format 为 datax 时必选
    * 默认值：无
    * 示例:`{ "usage": 5 }`

* `timeIndex`
    * 描述：时间字段对应输入列的序号，无符号整数
    * 必选：format 为 datax 时必选
    * 默认值：`http://127.0.0.1:8902/api/v1/write`
    * 示例：`1`

* `precision`
    * 描述：输入数据的时间戳精度，字符串
    * 必选：否
    * 默认值：`ms`
    * 可选值：`ms`, `us`, `ns`

## 4 类型转换

下表列出 DataX 的内部类型对应的 CnosDB 数据类型。

| DATAX 内部类型      | CNOSDB 数据类型           |
|-----------------|-----------------------|
| Date （time 列）   | TIMESTAMP(NANOSECOND) |
| Date （非 time 列） | BIGINT                |
| Long            | BIGINT                |
| Double          | DOUBLE                |
| Bytes           | 不支持                   |
| String          | STRING                |
| Bool            | BOOLEAN               |

## 5 性能报告

> - 软硬件环境，系统版本，java版本，CPU、内存等。
> - 数据特征，记录大小等。
> - 测试参数集（多组），系统参数（比如并发数），插件参数（比如batchSize）
> - 不同参数下同步速度（Rec/s, MB/s），机器负载（load, cpu）等，对数据源压力（load, cpu, mem等）。

## 6 约束限制

- 创建表时，TIMESTAMP 类型的列会被自动创建，列名为 time，用户无法创建额外的 TIMESTAMP 类型的列。非 time 列的 Datax DATE
  类型会被转换为 CnosDB BIGINT 类型。

## 6 FAQ

> 用户经常会遇到的问题。