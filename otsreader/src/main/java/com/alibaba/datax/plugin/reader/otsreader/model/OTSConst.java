package com.alibaba.datax.plugin.reader.otsreader.model;

public class OTSConst {
    // Reader support type
    public final static String TYPE_STRING  = "STRING";
    public final static String TYPE_INTEGER = "INT";
    public final static String TYPE_DOUBLE  = "DOUBLE";
    public final static String TYPE_BOOLEAN = "BOOL";
    public final static String TYPE_BINARY  = "BINARY";
    public final static String TYPE_INF_MIN = "INF_MIN";
    public final static String TYPE_INF_MAX = "INF_MAX";
    
    // Column
    public final static String NAME = "name";
    public final static String TYPE = "type";
    public final static String VALUE = "value";
    
    public final static String OTS_CONF = "OTS_CONF";
    public final static String OTS_RANGE = "OTS_RANGE";
    public final static String OTS_DIRECTION = "OTS_DIRECTION";
    
    // options
    public final static String RETRY = "maxRetryTime";
    public final static String SLEEP_IN_MILLI_SECOND = "retrySleepInMillionSecond";
}
