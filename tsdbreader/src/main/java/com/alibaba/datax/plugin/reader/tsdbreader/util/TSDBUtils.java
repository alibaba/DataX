package com.alibaba.datax.plugin.reader.tsdbreader.util;


/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：TSDB Utils
 *
 * @author Benedict Jin
 * @since 2019-10-21
 */
public final class TSDBUtils {

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
}
