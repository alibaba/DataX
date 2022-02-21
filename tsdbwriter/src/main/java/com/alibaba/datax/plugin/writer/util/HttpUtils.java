package com.alibaba.datax.plugin.writer.util;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
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

    public final static int CONNECT_TIMEOUT_DEFAULT_IN_MILL = (int) TimeUnit.SECONDS.toMillis(60);
    public final static int SOCKET_TIMEOUT_DEFAULT_IN_MILL = (int) TimeUnit.SECONDS.toMillis(60);

    private static final String CREDENTIALS_FORMAT = "%s:%s";
    private static final String BASIC_AUTHENTICATION_FORMAT = "Basic %s";

    private HttpUtils() {
    }

    public static String get(String url, String username, String password) throws Exception {
        final Request request = Request.Get(url)
                .connectTimeout(CONNECT_TIMEOUT_DEFAULT_IN_MILL)
                .socketTimeout(SOCKET_TIMEOUT_DEFAULT_IN_MILL);
        addAuth(request, username, password);
        Content content = request
                .execute()
                .returnContent();
        if (content == null) {
            return null;
        }
        return content.asString(StandardCharsets.UTF_8);
    }

    public static String post(String url, String username, String password, Map<String, Object> params) throws Exception {
        return post(url, username, password, JSON.toJSONString(params), CONNECT_TIMEOUT_DEFAULT_IN_MILL, SOCKET_TIMEOUT_DEFAULT_IN_MILL);
    }

    public static String post(String url, String username, String password, String params) throws Exception {
        return post(url, username, password, params, CONNECT_TIMEOUT_DEFAULT_IN_MILL, SOCKET_TIMEOUT_DEFAULT_IN_MILL);
    }

    public static String post(String url, String username, String password, String params,
                              int connectTimeoutInMill, int socketTimeoutInMill) throws Exception {
        Request request = Request.Post(url)
                .connectTimeout(connectTimeoutInMill)
                .socketTimeout(socketTimeoutInMill);
        addAuth(request, username, password);
        Content content = request
                .addHeader("Content-Type", "application/json")
                .bodyString(params, ContentType.APPLICATION_JSON)
                .execute()
                .returnContent();
        if (content == null) {
            return null;
        }
        return content.asString(StandardCharsets.UTF_8);
    }

    private static void addAuth(Request request, String username, String password) {
        String authorization = generateHttpAuthorization(username, password);
        if (authorization != null) {
            request.setHeader("Authorization", authorization);
        }
    }

    private static String generateHttpAuthorization(String username, String password) {
        if (StringUtils.isBlank(username) || StringUtils.isBlank(password)) {
            return null;
        }
        String credentials = String.format(CREDENTIALS_FORMAT, username, password);
        credentials = Base64.getEncoder().encodeToString(credentials.getBytes());
        return String.format(BASIC_AUTHENTICATION_FORMAT, credentials);
    }
}
