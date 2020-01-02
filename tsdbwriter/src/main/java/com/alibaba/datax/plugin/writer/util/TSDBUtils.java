package com.alibaba.datax.plugin.writer.util;

import com.alibaba.datax.plugin.writer.conn.DataPoint4TSDB;
import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：TSDB Utils
 *
 * @author Benedict Jin
 * @since 2019-03-29
 */
public final class TSDBUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TSDBUtils.class);

    private TSDBUtils() {
    }

    public static String version(String address) {
        String url = String.format("%s/api/version", address);
        String rsp;
        try {
            rsp = HttpUtils.get(url);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rsp;
    }

    public static String config(String address) {
        String url = String.format("%s/api/config", address);
        String rsp;
        try {
            rsp = HttpUtils.get(url);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rsp;
    }

    public static boolean put(String address, List<DataPoint4TSDB> dps) {
        return put(address, JSON.toJSON(dps));
    }

    public static boolean put(String address, DataPoint4TSDB dp) {
        return put(address, JSON.toJSON(dp));
    }

    private static boolean put(String address, Object o) {
        return put(address, o.toString());
    }

    public static boolean put(String address, String s) {
        String url = String.format("%s/api/put", address);
        String rsp;
        try {
            rsp = HttpUtils.post(url, s);
            // If successful, the returned content should be null.
            assert rsp == null;
        } catch (Exception e) {
            LOG.error("Address: {}, DataPoints: {}", url, s);
            throw new RuntimeException(e);
        }
        return true;
    }
}
