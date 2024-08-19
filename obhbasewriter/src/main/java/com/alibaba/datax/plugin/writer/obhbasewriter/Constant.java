package com.alibaba.datax.plugin.writer.obhbasewriter;

import ch.qos.logback.classic.Level;

public final class Constant {
    public static final String DEFAULT_ENCODING = "UTF-8";
    public static final String DEFAULT_DATA_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DEFAULT_NULL_MODE = "skip";
    public static final long DEFAULT_WRITE_BUFFER_SIZE = 8 * 1024 * 1024;
    public static final long DEFAULT_MEMSTORE_CHECK_INTERVAL_SECOND = 30;
    public static final double DEFAULT_MEMSTORE_THRESHOLD = 0.9d;
    public static final int DEFAULT_FAIL_TRY_COUNT = 10000;
    public static final String OB_TABLE_CLIENT_PROPERTY = "logging.path.com.alipay.oceanbase-table-client";
    public static final String OB_TABLE_HBASE_PROPERTY = "logging.path.com.alipay.oceanbase-table-hbase";
    public static final String OB_TABLE_CLIENT_LOG_LEVEL = "logging.level.oceanbase-table-client";
    public static final String OB_TABLE_HBASE_LOG_LEVEL = "logging.level.oceanbase-table-hbase";
    public static final String OB_COM_ALIPAY_TABLE_CLIENT_LOG_LEVEL = "logging.level.com.alipay.oceanbase-table-client";
    public static final String OB_COM_ALIPAY_TABLE_HBASE_LOG_LEVEL = "logging.level.com.alipay.oceanbase-table-hbase";
    public static final String OB_HBASE_LOG_PATH = System.getProperty("datax.home") + "/log/";
    public static final String DEFAULT_OB_TABLE_CLIENT_LOG_LEVEL = Level.OFF.toString();
    public static final String DEFAULT_OB_TABLE_HBASE_LOG_LEVEL = Level.OFF.toString();
    public static final String DEFAULT_NETTY_BUFFER_LOW_WATERMARK = Integer.toString(512 * 1024);
    public static final String DEFAULT_NETTY_BUFFER_HIGH_WATERMARK = Integer.toString(1024 * 1024);
    public static final String DEFAULT_HBASE_HTABLE_CLIENT_WRITE_BUFFER = "2097152";
    public static final String DEFAULT_HBASE_HTABLE_PUT_WRITE_BUFFER_CHECK = "10";
    public static final String DEFAULT_RPC_EXECUTE_TIMEOUT = "3000";
}
