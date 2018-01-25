package com.alibaba.datax.plugin.reader.otsstreamreader.internal.config;

import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.TimeUtils;

public class OTSStreamReaderConstants {

    public static long BEFORE_OFFSET_TIME_MILLIS = 10 * TimeUtils.MINUTE_IN_MILLIS;

    public static long AFTER_OFFSET_TIME_MILLIS = 5 * TimeUtils.MINUTE_IN_MILLIS;

    public static final int STATUS_TABLE_TTL = 30 * TimeUtils.DAY_IN_SEC;

    public static final long MAX_WAIT_TABLE_READY_TIME_MILLIS = 2 * TimeUtils.MINUTE_IN_MILLIS;

    public static final long MAX_OTS_UNAVAILABLE_TIME = 30 * TimeUtils.MINUTE_IN_MILLIS;

    public static final long MAX_ONCE_PROCESS_TIME_MILLIS = MAX_OTS_UNAVAILABLE_TIME;

    public static final String CONF = "conf";

    public static final String STREAM_JOB = "STREAM_JOB";
    public static final String OWNED_SHARDS = "OWNED_SHARDS";
    public static final String ALL_SHARDS = "ALL_SHARDS";


    static {
        String beforeOffsetMillis = System.getProperty("BEFORE_OFFSET_TIME_MILLIS");
        if (beforeOffsetMillis != null) {
            BEFORE_OFFSET_TIME_MILLIS = Long.valueOf(beforeOffsetMillis);
        }

        String afterOffsetMillis = System.getProperty("AFTER_OFFSET_TIME_MILLIS");
        if (afterOffsetMillis != null) {
            AFTER_OFFSET_TIME_MILLIS = Long.valueOf(afterOffsetMillis);
        }
    }
}
