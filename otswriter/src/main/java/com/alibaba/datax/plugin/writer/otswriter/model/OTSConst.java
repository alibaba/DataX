package com.alibaba.datax.plugin.writer.otswriter.model;

public class OTSConst {
    // Reader support type
    public final static String TYPE_STRING  = "STRING";
    public final static String TYPE_INTEGER = "INT";
    public final static String TYPE_DOUBLE  = "DOUBLE";
    public final static String TYPE_BOOLEAN = "BOOL";
    public final static String TYPE_BINARY  = "BINARY";
    
    // Column
    public final static String NAME = "name";
    public final static String TYPE = "type";
    
    public final static String OTS_CONF = "OTS_CONF";
    
    public final static String OTS_OP_TYPE_PUT = "PutRow";
    public final static String OTS_OP_TYPE_UPDATE = "UpdateRow";
    public final static String OTS_OP_TYPE_DELETE = "DeleteRow";

    // options
    public final static String RETRY = "maxRetryTime";
    public final static String SLEEP_IN_MILLISECOND = "retrySleepInMillisecond";
    public final static String BATCH_WRITE_COUNT = "batchWriteCount";
    public final static String CONCURRENCY_WRITE = "concurrencyWrite";
    public final static String IO_THREAD_COUNT = "ioThreadCount";
    public final static String SOCKET_TIMEOUT = "socketTimeoutInMillisecond";
    public final static String CONNECT_TIMEOUT = "connectTimeoutInMillisecond";
    public final static String BUFFER_SIZE = "bufferSize";
    
    // 限制项
    public final static String REQUEST_TOTAL_SIZE_LIMITATION = "requestTotalSizeLimitation";
    public final static String ATTRIBUTE_COLUMN_SIZE_LIMITATION = "attributeColumnSizeLimitation";
    public final static String PRIMARY_KEY_COLUMN_SIZE_LIMITATION = "primaryKeyColumnSizeLimitation";
    public final static String ATTRIBUTE_COLUMN_MAX_COUNT = "attributeColumnMaxCount";
}