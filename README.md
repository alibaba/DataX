![Datax-logo](https://github.com/alibaba/DataX/blob/master/images/DataX-logo.jpg)



# DataX

DataX 是阿里巴巴集团内被广泛使用的离线数据同步工具/平台，实现包括 MySQL、Oracle、SqlServer、Postgre、HDFS、Hive、ADS、HBase、TableStore(OTS)、MaxCompute(ODPS)、DRDS 等各种异构数据源之间高效的数据同步功能。


此版本DataX是为了适应人人车数据平台的需要对阿里巴巴开源的DataX进行调整而成的版本。

# ChangeLog
1. Clickhouse支持
    * 增加 ClickhouseReader 与 ClickhouseWriter 支撑对 [Clickhouse](https://clickhouse.yandex/) 的读写操作
2. 去除因jar包缺失而编译不通过的otsstreamreader插件
3. HDFS writer：
    * 增加 replace 模式，支持文件直接替换
    * 增加指定用户名选项
