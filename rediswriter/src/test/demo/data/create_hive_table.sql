-- 执行创建表的语句后把测试数据hive_data.txt文件上传到表对应的路径
CREATE table redis_writer(uid int,channels string,name string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;