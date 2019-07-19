package com.alibaba.datax.plugin.reader.hbase20xsqlreader;

public class Key {
    /**
     * 【必选】writer要读取的表的表名
     */
    public final static String TABLE = "table";
    /**
     * 【必选】writer要读取哪些列
     */
    public final static String COLUMN = "column";
    /**
     * 【必选】Phoenix QueryServer服务地址
     */
    public final static String QUERYSERVER_ADDRESS = "queryServerAddress";
    /**
     * 【可选】序列化格式，默认为PROTOBUF
     */
    public static final String SERIALIZATION_NAME = "serialization";
    /**
     * 【可选】Phoenix表所属schema，默认为空
     */
    public static final String SCHEMA = "schema";
    /**
     * 【可选】读取数据时切分列
     */
    public static final String SPLIT_KEY = "splitKey";
    /**
     * 【可选】读取数据时切分点
     */
    public static final String SPLIT_POINT = "splitPoint";
    /**
     * 【可选】读取数据过滤条件配置
     */
    public static final String WHERE = "where";
    /**
     * 【可选】查询语句配置
     */
    public static final String QUERY_SQL = "querySql";
}
