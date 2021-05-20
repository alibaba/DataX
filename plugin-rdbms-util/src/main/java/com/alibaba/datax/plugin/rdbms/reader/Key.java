package com.alibaba.datax.plugin.rdbms.reader;

/**
 * 编码，时区等配置，暂未定.
 */
public final class Key {
    public final static String JDBC_URL = "jdbcUrl";

    public final static String USERNAME = "username";

    public final static String PASSWORD = "password";

    public final static String TABLE = "table";
    
    public final static String MANDATORY_ENCODING = "mandatoryEncoding";

    // 是数组配置
    public final static String COLUMN = "column";
    
    public final static String COLUMN_LIST = "columnList";

    public final static String WHERE = "where";

    public final static String HINT = "hint";

    public final static String SPLIT_PK = "splitPk";
    
    public final static String SPLIT_MODE = "splitMode";
    
    public final static String SAMPLE_PERCENTAGE = "samplePercentage";

    public final static String QUERY_SQL = "querySql";

    public final static String SPLIT_PK_SQL = "splitPkSql";


    public final static String PRE_SQL = "preSql";

    public final static String POST_SQL = "postSql";

    public final static String CHECK_SLAVE = "checkSlave";

	public final static String SESSION = "session";

	public final static String DBNAME = "dbName";

    public final static String DRYRUN = "dryRun";

    public static String SPLIT_FACTOR = "splitFactor";

    public final static String WEAK_READ = "weakRead";

    public final static String SAVE_POINT = "savePoint";

    public final static String REUSE_CONN = "reuseConn";

    public final static String PARTITION_NAME = "partitionName";
}
