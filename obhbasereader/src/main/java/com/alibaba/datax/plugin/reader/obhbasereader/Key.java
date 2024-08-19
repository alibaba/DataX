package com.alibaba.datax.plugin.reader.obhbasereader;

public final class Key {

    public final static String HBASE_CONFIG = "hbaseConfig";

    /**
     * mode 可以取 normal 或者 multiVersionFixedColumn 或者 multiVersionDynamicColumn 三个值，无默认值。
     * <p/>
     * normal 配合 column(Map 结构的)使用
     * <p/>
     * multiVersionFixedColumn 配合 maxVersion,tetradType, column(List 结构的)使用
     * <p/>
     * multiVersionDynamicColumn 配合 maxVersion,tetradType, columnFamily(List 结构的)使用
     */
    public final static String MODE = "mode";

    /**
     * 配合 mode = multiVersion 时使用，指明需要读取的版本个数。无默认值
     * -1 表示去读全部版本
     * 不能为0，1
     * >1 表示最多读取对应个数的版本数(不能超过 Integer 的最大值)
     */
    public final static String MAX_VERSION = "maxVersion";

    /**
     * 多版本情况下，必须配置 四元组的类型(rowkey,column,timestamp,value)
     */
    public final static String TETRAD_TYPE = "tetradType";

    /**
     * 默认为 utf8
     */
    public final static String ENCODING = "encoding";

    public final static String TABLE = "table";

    public final static String USERNAME = "username";

    public final static String OB_SYS_USERNAME = "obSysUser";

    public final static String CONFIG_URL = "obConfigUrl";

    public final static String ODP_HOST = "odpHost";

    public final static String ODP_PORT = "odpPort";

    public final static String DB_NAME = "dbName";

    public final static String PASSWORD = "password";

    public final static String OB_SYS_PASSWORD = "obSysPassword";

    public final static String COLUMN_FAMILY = "columnFamily";

    public final static String COLUMN = "column";

    public final static String START_ROWKEY = "startRowkey";

    public final static String END_ROWKEY = "endRowkey";

    public final static String IS_BINARY_ROWKEY = "isBinaryRowkey";

    public final static String SCAN_CACHE = "scanCache";

    public final static String RS_URL = "rsUrl";

    public final static String MAX_ACTIVE_CONNECTION = "maxActiveConnection";

    public final static int DEFAULT_MAX_ACTIVE_CONNECTION = 2000;

    public final static String TIMEOUT = "timeout";

    public final static long DEFAULT_TIMEOUT = 30;

    public final static String PARTITION_NAME = "partitionName";

    public final static String JDBC_URL = "jdbcUrl";

    public final static String TIMEZONE = "timezone";

    public final static String FETCH_SIZE = "fetchSize";

    public final static String READ_BATCH_SIZE = "readBatchSize";

    public final static String SESSION = "session";

    public final static String READER_HINT = "readerHint";

    public final static String QUERY_SQL = "querySql";

    public final static String SAMPLE_PERCENTAGE = "samplePercentage";
    // 是否使用独立密码
    public final static String USE_SPECIAL_SECRET = "useSpecialSecret";

    public final static String USE_SQL_READER = "useSqlReader";

    public final static String USE_ODP_MODE = "useOdpMode";

    public final static String RANGE = "range";

    public final static String READ_BY_PARTITION = "readByPartition";
}
