package com.alibaba.datax.plugin.writer.hbase20xsqlwriter;

public final class Constant {
    public static final String DEFAULT_NULL_MODE = "skip";
    public static final String DEFAULT_SERIALIZATION = "PROTOBUF";
    public static final int DEFAULT_BATCH_ROW_COUNT = 256;   // 默认一次写256行

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
