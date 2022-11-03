# DataX XuguWriter


---


## 1 快速介绍

XuguWriter 插件实现了写入数据到 Xugu 主库的目的表的功能。在底层实现上， XuguWriter 通过 JDBC 连接远程 Xugu 数据库，并执行相应的 insert into ... 或者 ( replace into ...) 的 sql 语句将数据写入 Xugu，内部会分批次提交入库

XuguWriter 面向ETL开发工程师，他们使用 XuguWriter 从数仓导入数据到 Xugu。同时 XuguWriter 亦可以作为数据迁移工具为DBA等用户提供服务。


## 2 实现原理

XuguWriter 通过 DataX 框架获取 Reader 生成的协议数据，根据你配置的 `writeMode` 生成


* `insert into...`(当主键/唯一性索引冲突时会写不进去冲突的行)

##### 或者

* `replace into...`(没有遇到主键/唯一性索引冲突时，与 insert into 行为一致，冲突时会用新行替换原有行所有字段) 的语句写入数据到 Xugu。出于性能考虑，采用了 `PreparedStatement + Batch`，将数据缓冲到线程上下文 Buffer 中，当 Buffer 累计到预定阈值时，才发起写入请求。

<br />

    注意：目的表所在数据库必须是主库才能写入数据；整个任务至少需要具备 insert/replace into...的权限，是否需要其他权限，取决于你任务配置中在 preSql 和 postSql 中指定的语句。


## 3 功能说明

### 3.1 配置样例

* 这里使用一份从Xugu源端迁移到 Xugu目标端 导入的样例。

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
					"name": "xugureader",
					"parameter": {
						"username": "SYSDBA",
						"password": "SYSDBA",
						"column": ["*"],
						"splitPk": "id",
						"connection": [
							{
								"table": [
									"CM_SESSION_TOTAL"
								],
								"jdbcUrl": [
									"jdbc:xugu://192.168.2.214:5151/SYSTEM"
								]
							}
						]
					}
				},
				"writer": {
					"name": "xuguwriter",
					"parameter": {
						"writeMode": "insert",
						"username": "SYSDBA",
						"password": "SYSDBA",
						"column": ["*"],
						"preSql": [
							"truncate table CM_SESSION_TOTAL"
						],
						"connection": [
							{
								"jdbcUrl": "jdbc:xugu://192.168.2.126:5151/datax",
								"table": [
									"CM_SESSION_TOTAL"
								]
							}
						]
					}
				}
			}
		]
	}
}

```

### 3.2 参数说明

* **jdbcUrl**

	* 描述：目的数据库的 JDBC 连接信息 ,jdbcUrl必须包含在connection配置单元中。

               注意：1、jdbcUrl按照Xugu官方规范，并可以填写连接附加控制信息，具体请参看 Xugu官方文档或者咨询对应 DBA。


 	* 必选：是 <br />

	* 默认值：无 <br />

* **username**

	* 描述：目的数据库的用户名 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **password**

	* 描述：目的数据库的密码 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **table**

	* 描述：目的表的表名称。支持写入一个或者多个表。当配置为多张表时，必须确保所有表结构保持一致。

               注意：table 和 jdbcUrl 必须包含在 connection 配置单元中

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。如果要依次写入全部列，使用`*`表示, 例如: `"column": ["*"]`。

			**column配置项必须指定，不能留空！**

               注意：1、我们强烈不推荐你这样配置，因为当你目的表字段个数、类型等有改动时，你的任务可能运行不正确或者失败
                    2、 column 不能配置任何常量值

	* 必选：是 <br />

	* 默认值：否 <br />

* **preSql**

	* 描述：写入数据到目的表前，会先执行这里的标准语句。如果 Sql 中有你需要操作到的表名称，请使用 `@table` 表示，这样在实际执行 Sql 语句时，会对变量按照实际表名称进行替换。比如你的任务是要写入到目的端的100个同构分表(表名称为:datax_00,datax01, ... datax_98,datax_99)，并且你希望导入数据前，先对表中数据进行删除操作，那么你可以这样配置：`"preSql":["delete from 表名"]`，效果是：在执行到每个表写入数据前，会先执行对应的 delete from 对应表名称 <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **postSql**

	* 描述：写入数据到目的表后，会执行这里的标准语句。（原理同 preSql ） <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **writeMode**

	* 描述：控制写入数据到目标表采用 `insert into` 或者 `replace into`  语句<br />

	* 必选：是 <br />
	
	* 所有选项：insert/replace <br />

	* 默认值：insert <br />

* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与Xugu的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。<br />

	* 必选：否 <br />

	* 默认值：1024 <br />


### 3.3 类型转换

类似 XuguReader ，目前 XuguWriter 支持大部分 Xugu 类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出 XuguWriter 针对 Xugu 类型转换列表:


| DataX 内部类型| Xugu 数据类型                                   |
| -------- |---------------------------------------------|
| Long     | int, tinyint, smallint, integer, bigint |
| Double   | float, double, numeric                      |
| String   | varchar, char, clob                         |
| Date     | date, datetime, timestamp, time             |
| Boolean  | boolean                                     |
| Bytes    | blob, binary                                |



## 4 约束限制


## FAQ

***

**Q: XuguWriter 执行 postSql 语句报错，那么数据导入到目标数据库了吗?**

A: DataX 导入过程存在三块逻辑，pre 操作、导入操作、post 操作，其中任意一环报错，DataX 作业报错。由于 DataX 不能保证在同一个事务完成上述几个操作，因此有可能数据已经落入到目标端。

***

**Q: 按照上述说法，那么有部分脏数据导入数据库，如果影响到线上数据库怎么办?**

A: 目前有两种解法，第一种配置 pre 语句，该 sql 可以清理当天导入数据， DataX 每次导入时候可以把上次清理干净并导入完整数据。第二种，向临时表导入数据，完成后再 rename 到线上表。

***

**Q: 上面第二种方法可以避免对线上数据造成影响，那我具体怎样操作?**

A: 可以配置临时表导入
