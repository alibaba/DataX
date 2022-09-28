package com.alibaba.datax.plugin.reader.influxdbreader;

/**
 * Function: Key
 * @author Shenguanchu
 * @since 2022-09-21
 */
public class Key {
    public static final String CONNECTION = "connection";
    public static final String URL = "url";
    public static final String TOKEN = "token";
    public static final String ORG = "org";
    public static final String BUCKET = "bucket";
    public static final String BEGIN_DATETIME = "beginDateTime";
    public static final String END_DATETIME = "endDateTime";
    static final String INTERVAL_DATE_TIME = "splitIntervalH";
    static final Integer INTERVAL_DATE_TIME_DEFAULT_VALUE = 240;
    static final String MINI_TASK_INTERVAL_SECOND = "miniTaskIntervalSecond";
    static final String READE_TIMEOUT = "readTimeout";
    static final String WRITE_TIMEOUT = "writeTimeout";
    static final String CONNECT_TIMEOUT = "connectTimeout";
}
