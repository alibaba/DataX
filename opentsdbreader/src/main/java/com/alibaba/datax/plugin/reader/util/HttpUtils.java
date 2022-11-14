package com.alibaba.datax.plugin.reader.util;

import com.alibaba.fastjson2.JSON;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.TimeUnit;

//This file is part of OpenTSDB.

//Copyright (C) 2010-2012  The OpenTSDB Authors.
//Copyright（C）2019 Alibaba Group Holding Ltd.

//

//This program is free software: you can redistribute it and/or modify it

//under the terms of the GNU Lesser General Public License as published by

//the Free Software Foundation, either version 2.1 of the License, or (at your

//option) any later version.  This program is distributed in the hope that it

//will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty

//of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser

//General Public License for more details.  You should have received a copy

//of the GNU Lesser General Public License along with this program.  If not,

//see <http://www.gnu.org/licenses/>.
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
