package com.alibaba.datax.plugin.writer.clickhousewriter;

public final class Key {

    public final static String JDBC_URL = "jdbcUrl";

    public final static String USERNAME = "username";

    public final static String PASSWORD = "password";

    public final static String TABLE = "table";

    public final static String COLUMN = "column";

    public final static String PRE_SQL = "preSql";

    public final static String POST_SQL = "postSql";
    //默认值：1024
    public final static String BATCH_SIZE = "batchSize";
    //默认值：32m
    public final static String BATCH_BYTE_SIZE = "batchByteSize";

    public final static String EMPTY_AS_NULL = "emptyAsNull";
}
