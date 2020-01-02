package com.alibaba.datax.plugin.reader.conn;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.fastjson.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

import java.util.Map;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：OpenTSDB Dump
 *
 * @author Benedict Jin
 * @since 2019-04-15
 */
final class OpenTSDBDump {

    private static TSDB TSDB_INSTANCE;

    private OpenTSDBDump() {
    }

    static void dump(OpenTSDBConnection conn, String metric, Long start, Long end, RecordSender sender) throws Exception {
        DumpSeries.doDump(getTSDB(conn), new String[]{start + "", end + "", "none", metric}, sender);
    }

    private static TSDB getTSDB(OpenTSDBConnection conn) {
        if (TSDB_INSTANCE == null) {
            synchronized (TSDB.class) {
                if (TSDB_INSTANCE == null) {
                    try {
                        Config config = new Config(false);
                        Map configurations = JSON.parseObject(conn.config(), Map.class);
                        for (Object key : configurations.keySet()) {
                            config.overrideConfig(key.toString(), configurations.get(key.toString()).toString());
                        }
                        TSDB_INSTANCE = new TSDB(config);
                    } catch (Exception e) {
                        throw new RuntimeException("Cannot init OpenTSDB connection!");
                    }
                }
            }
        }
        return TSDB_INSTANCE;
    }
}
