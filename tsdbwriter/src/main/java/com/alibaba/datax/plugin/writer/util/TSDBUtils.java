package com.alibaba.datax.plugin.writer.util;

import com.alibaba.datax.plugin.writer.conn.DataPoint4TSDB;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
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

    public static String version(String address, String username, String password) {
        String url = String.format("%s/api/version", address);
        String rsp;
        try {
            rsp = HttpUtils.get(url, username, password);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rsp;
    }

    public static String config(String address, String username, String password) {
        String url = String.format("%s/api/config", address);
        String rsp;
        try {
            rsp = HttpUtils.get(url, username, password);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rsp;
    }

    public static boolean put(String address, String database, String username, String password, List<DataPoint4TSDB> dps) {
        return put(address, database, username, password, JSON.toJSON(dps));
    }

    public static boolean put(String address, String database, String username, String password, DataPoint4TSDB dp) {
        return put(address, database, username, password, JSON.toJSON(dp));
    }

    private static boolean put(String address, String database, String username, String password, Object o) {
        return put(address, database, username, password, o.toString());
    }

    public static boolean put(String address, String database, String username, String password, String s) {
        return put(address, database, username, password, s, false);
    }

    public static boolean mput(String address, String database, String username, String password, String s) {
        return put(address, database, username, password, s, true);
    }

    public static boolean put(String address, String database, String username, String password, String s, boolean multiField) {
        String url = address + (multiField ? "/api/mput" : "/api/put");
        if (!StringUtils.isBlank(database)) {
            url = url.concat("?db=" + database);
        }
        String rsp;
        try {
            rsp = HttpUtils.post(url, username, password, s);
            // If successful, the returned content should be null.
            assert rsp == null;
        } catch (Exception e) {
            LOG.error("Address: {}, DataPoints: {}", url, s);
            throw new RuntimeException(e);
        }
        return true;
    }
}
