package com.alibaba.datax.plugin.writer.otswriter.model;

public class OTSConst {
    // Reader support type
    public final static String TYPE_STRING = "STRING";
    public final static String TYPE_INTEGER = "INT";
    public final static String TYPE_DOUBLE = "DOUBLE";
    public final static String TYPE_BOOLEAN = "BOOL";
    public final static String TYPE_BINARY = "BINARY";

    // Column
    public final static String NAME = "name";
    public final static String SRC_NAME = "srcName";
    public final static String TYPE = "type";
    public final static String IS_TAG = "is_timeseries_tag";

    public final static String OTS_CONF = "OTS_CONF";

    public final static String OTS_MODE_NORMAL = "normal";
    public final static String OTS_MODE_MULTI_VERSION = "multiVersion";
    public final static String OTS_MODE_TIME_SERIES = "timeseries";

    public final static String OTS_OP_TYPE_PUT = "PutRow";
    public final static String OTS_OP_TYPE_UPDATE = "UpdateRow";
    // only support in old version
    public final static String OTS_OP_TYPE_DELETE = "DeleteRow";

    // options
    public final static String RETRY = "maxRetryTime";
    public final static String SLEEP_IN_MILLISECOND = "retrySleepInMillisecond";
    public final static String BATCH_WRITE_COUNT = "batchWriteCount";
    public final static String CONCURRENCY_WRITE = "concurrencyWrite";
    public final static String IO_THREAD_COUNT = "ioThreadCount";
    public final static String MAX_CONNECT_COUNT = "maxConnectCount";
    public final static String SOCKET_TIMEOUTIN_MILLISECOND = "socketTimeoutInMillisecond";
    public final static String CONNECT_TIMEOUT_IN_MILLISECOND = "connectTimeoutInMillisecond";
    public final static String REQUEST_TOTAL_SIZE_LIMITATION = "requestTotalSizeLimitation";

    public static final String MEASUREMENT_NAME = "_m_name";
    public static final String DATA_SOURCE = "_data_source";
    public static final String TAGS = "_tags";
    public static final String TIME = "_time";
}
