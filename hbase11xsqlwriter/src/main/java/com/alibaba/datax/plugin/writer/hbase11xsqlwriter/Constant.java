package com.alibaba.datax.plugin.writer.hbase11xsqlwriter;

public final class Constant {
    public static final String DEFAULT_ENCODING = "UTF-8";
    public static final String DEFAULT_DATA_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DEFAULT_NULL_MODE = "skip";
    public static final String DEFAULT_ZNODE = "/hbase";
    public static final boolean DEFAULT_LAST_COLUMN_IS_VERSION = false;   // 默认最后一列不是version列
    public static final int DEFAULT_BATCH_ROW_COUNT = 256;   // 默认一次写256行
    public static final boolean DEFAULT_TRUNCATE = false;    // 默认开始的时候不清空表

    public static final int TYPE_UNSIGNED_TINYINT = 11;
    public static final int TYPE_UNSIGNED_SMALLINT = 13;
    public static final int TYPE_UNSIGNED_INTEGER = 9;
    public static final int TYPE_UNSIGNED_LONG = 10;
    public static final int TYPE_UNSIGNED_FLOAT = 14;
    public static final int TYPE_UNSIGNED_DOUBLE = 15;
    public static final int TYPE_UNSIGNED_DATE = 19;
    public static final int TYPE_UNSIGNED_TIME = 18;
    public static final int TYPE_UNSIGNED_TIMESTAMP = 20;
}
