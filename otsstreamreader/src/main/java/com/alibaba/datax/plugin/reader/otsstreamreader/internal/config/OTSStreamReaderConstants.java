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
    public static final String VERSION = "STREAM_VERSION";

    /**
     * 是否开启OTS分布式模式降低Job Split阶段切分的Task Conf大小启动优化，
     * 新增该参数的目的是为了保证DataX灰度过程，避免因为OTS分布式任务运行部分子进程运行在老版本、部分运行在新版本导致任务失败问题，
     * 当DataX版本集群粒度已全量升级到新版本以后，再开启该参数为"true"，默认值是"false"
     */
    public static final String CONF_SIMPLIFY_ENABLE = "confSimplifyEnable";

    public static final Integer RETRY_TIMES = 3;

    public static final Long DEFAULT_SLEEP_TIME_IN_MILLS = 500l;

    public static final boolean DEFAULT_CONF_SIMPLIFY_ENABLE_VALUE = false;

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
