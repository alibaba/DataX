package com.alibaba.datax.plugin.writer.util;

import com.alibaba.fastjson.JSON;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：HttpUtils
 *
 * @author Benedict Jin
 * @since 2019-03-29
 */
public final class HttpUtils {

    public final static Charset UTF_8 = Charset.forName("UTF-8");
    public final static int CONNECT_TIMEOUT_DEFAULT_IN_MILL = (int) TimeUnit.SECONDS.toMillis(60);
    public final static int SOCKET_TIMEOUT_DEFAULT_IN_MILL = (int) TimeUnit.SECONDS.toMillis(60);

    private HttpUtils() {
    }

    public static String get(String url) throws Exception {
        Content content = Request.Get(url)
                .connectTimeout(CONNECT_TIMEOUT_DEFAULT_IN_MILL)
                .socketTimeout(SOCKET_TIMEOUT_DEFAULT_IN_MILL)
                .execute()
                .returnContent();
        if (content == null) {
            return null;
        }
        return content.asString(UTF_8);
    }

    public static String post(String url, Map<String, Object> params) throws Exception {
        return post(url, JSON.toJSONString(params), CONNECT_TIMEOUT_DEFAULT_IN_MILL, SOCKET_TIMEOUT_DEFAULT_IN_MILL);
    }

    public static String post(String url, String params) throws Exception {
        return post(url, params, CONNECT_TIMEOUT_DEFAULT_IN_MILL, SOCKET_TIMEOUT_DEFAULT_IN_MILL);
    }

    public static String post(String url, Map<String, Object> params,
                              int connectTimeoutInMill, int socketTimeoutInMill) throws Exception {
        return post(url, JSON.toJSONString(params), connectTimeoutInMill, socketTimeoutInMill);
    }

    public static String post(String url, String params,
                              int connectTimeoutInMill, int socketTimeoutInMill) throws Exception {
        Content content = Request.Post(url)
                .connectTimeout(connectTimeoutInMill)
                .socketTimeout(socketTimeoutInMill)
                .addHeader("Content-Type", "application/json")
                .bodyString(params, ContentType.APPLICATION_JSON)
                .execute()
                .returnContent();
        if (content == null) {
            return null;
        }
        return content.asString(UTF_8);
    }
}
