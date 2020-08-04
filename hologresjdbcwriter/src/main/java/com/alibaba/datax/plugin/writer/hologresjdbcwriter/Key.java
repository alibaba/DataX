package com.alibaba.datax.plugin.writer.hologresjdbcwriter;

public final class Key {
    public final static String JDBC_URL = "jdbcUrl";

    public final static String USERNAME = "username";

    public final static String PASSWORD = "password";

    public final static String TABLE = "table";

    public final static String COLUMN = "column";

    public final static String PRIMARY_KEY = "primaryKey";

    public final static String Array_Type = "arrayType";

    public final static String Array_Delimiter = "arrayDelimiter";

    public final static String WRITE_MODE = "writeMode";

    public final static String PRE_SQL = "preSql";

    public final static String POST_SQL = "postSql";

    public final static String TDDL_APP_NAME = "appName";

    //默认值：256
    public final static String BATCH_SIZE = "batchSize";

    //默认值：32m
    public final static String BATCH_BYTE_SIZE = "batchByteSize";

    public final static String EMPTY_AS_NULL = "emptyAsNull";

    public final static String DB_NAME_PATTERN = "dbNamePattern";

    public final static String DB_RULE = "dbRule";

    public final static String TABLE_NAME_PATTERN = "tableNamePattern";

    public final static String TABLE_RULE = "tableRule";

    public final static String DRYRUN = "dryRun";
}