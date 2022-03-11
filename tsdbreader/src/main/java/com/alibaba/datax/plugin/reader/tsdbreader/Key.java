package com.alibaba.datax.plugin.reader.tsdbreader;

import java.util.HashSet;
import java.util.Set;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：Key
 *
 * @author Benedict Jin
 * @since 2019-10-21
 */
public class Key {

    // TSDB for OpenTSDB / InfluxDB / TimeScale / Prometheus etc.
    // RDB for MySQL / ADB etc.
    static final String SINK_DB_TYPE = "sinkDbType";
    static final String ENDPOINT = "endpoint";
    static final String USERNAME = "username";
    static final String PASSWORD = "password";
    static final String COLUMN = "column";
    static final String METRIC = "metric";
    static final String FIELD = "field";
    static final String TAG = "tag";
    static final String COMBINE = "combine";
    static final String INTERVAL_DATE_TIME = "splitIntervalMs";
    static final String BEGIN_DATE_TIME = "beginDateTime";
    static final String END_DATE_TIME = "endDateTime";
    static final String HINT = "hint";

    static final Boolean COMBINE_DEFAULT_VALUE = false;
    static final Integer INTERVAL_DATE_TIME_DEFAULT_VALUE = 60;
    static final String TYPE_DEFAULT_VALUE = "TSDB";
    static final Set<String> TYPE_SET = new HashSet<>();

    static {
        TYPE_SET.add("TSDB");
        TYPE_SET.add("RDB");
    }
}
