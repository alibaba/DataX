package com.alibaba.datax.plugin.writer.obhbasewriter;

public final class ConfigKey {

    public final static String HBASE_CONFIG = "hbaseConfig";

    public final static String TABLE = "table";

    public final static String DBNAME = "dbName";

    public final static String OBCONFIG_URL = "obConfigUrl";

    public final static String JDBC_URL = "jdbcUrl";
    /**
     * mode 可以取 normal 或者 multiVersionFixedColumn 或者 multiVersionDynamicColumn 三个值，无默认值。
     * <p/>
     * normal 配合 column(Map 结构的)使用
     * <p/>
     * multiVersion
     */
    public final static String MODE = "mode";

    public final static String ROWKEY_COLUMN = "rowkeyColumn";

    public final static String VERSION_COLUMN = "versionColumn";

    /**
     * 默认为 utf8
     */
    public final static String ENCODING = "encoding";

    public final static String COLUMN = "column";

    public static final String INDEX = "index";

    public static final String NAME = "name";

    public static final String TYPE = "type";

    public static final String VALUE = "value";

    public static final String FORMAT = "format";

    /**
     * 默认为 EMPTY_BYTES
     */
    public static final String NULL_MODE = "nullMode";

    public static final String TRUNCATE = "truncate";

    public static final String AUTO_FLUSH = "autoFlush";

    public static final String WAL_FLAG = "walFlag";

    public static final String WRITE_BUFFER_SIZE = "writeBufferSize";

    public static final String MAX_RETRY_COUNT = "maxRetryCount";

    public static final String USE_ODP_MODE = "useOdpMode";

    public static final String OB_SYS_USER = "obSysUser";

    public static final String OB_SYS_PASSWORD = "obSysPassword";

    public static final String ODP_HOST = "odpHost";

    public static final String ODP_PORT = "odpPort";

    public static final String OBHBASE_HTABLE_CLIENT_WRITE_BUFFER = "obhbaseClientWriteBuffer";

    public static final String OBHBASE_HTABLE_PUT_WRITE_BUFFER_CHECK = "obhbaseHtablePutWriteBufferCheck";

    public static final String WRITE_BUFFER_LOW_MARK = "writeBufferLowMark";

    public static final String WRITE_BUFFER_HIGH_MARK = "writeBufferHighMark";

    public static final String TABLE_CLIENT_RPC_EXECUTE_TIMEOUT = "rpcExecuteTimeout";
}
