package com.alibaba.datax.plugin.writer.hologresjdbcwriter;

/**
 * 用于插件解析用户配置时，需要进行标识（MARK）的常量的声明.
 */
public final class Constant {
    public static final int DEFAULT_BATCH_SIZE = 512;

    public static final int DEFAULT_BATCH_BYTE_SIZE = 50 * 1024 * 1024;

    public static String CONN_MARK = "connection";

    public static String TABLE_NUMBER_MARK = "tableNumber";

}
