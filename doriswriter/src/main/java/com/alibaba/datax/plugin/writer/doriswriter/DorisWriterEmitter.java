// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.alibaba.datax.plugin.writer.doriswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// Used to load batch of rows to Doris using stream load
public class DorisWriterEmitter {
    private static final Logger LOG = LoggerFactory.getLogger(DorisWriterEmitter.class);
    private final Key keys;
    private int hostPos = 0;
    private List<String> targetHosts = Lists.newArrayList();

    private RequestConfig requestConfig;

    public DorisWriterEmitter(final Key keys) {
        this.keys = keys;
        initHostList();
        initRequestConfig();
    }

    private void initRequestConfig() {
        requestConfig = RequestConfig.custom().setConnectTimeout(this.keys.getConnectTimeout()).build();
    }

    // get target host from config
    private void initHostList() {
        List<String> hosts = this.keys.getBeLoadUrlList();
        if (hosts == null || hosts.isEmpty()) {
            hosts = this.keys.getFeLoadUrlList();
        }
        if (hosts == null || hosts.isEmpty()) {
            DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                    "Either beLoadUrl or feLoadUrl must be set");
        }
        for (String beHost : hosts) {
            targetHosts.add("http://" + beHost);
        }
    }

    public void emit(final DorisFlushBatch flushData) {
        String host = this.getAvailableHost();
        for (int i = 0; i <= this.keys.getMaxRetries(); i++) {
            try {
                doStreamLoad(flushData, host);
                return;
            } catch (DataXException ex) {
                if (i >= this.keys.getMaxRetries()) {
                    throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, ex);
                }
                LOG.error("StreamLoad error, switch host {} and retry: ", host, ex);
                host = this.getAvailableHost();
            }
        }
    }

    /**
     * execute doris stream load
     */
    private void doStreamLoad(final DorisFlushBatch flushData, String host) {
        long start = System.currentTimeMillis();
        if (StringUtils.isEmpty(host)) {
            throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, "None of the load url can be connected.");
        }
        final String loadUrl = host + "/api/" + this.keys.getDatabase() + "/" + this.keys.getTable() + "/_stream_load";
        // do http put request and get response
        final Map<String, Object> loadResult;
        try {
            loadResult = this.doHttpPut(loadUrl, flushData);
        } catch (IOException e) {
            throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
        }

        long cost = System.currentTimeMillis() - start;
        LOG.info("StreamLoad response: " + JSON.toJSONString(loadResult) + ", cost(ms): " + cost);
        final String keyStatus = "Status";
        if (null == loadResult || !loadResult.containsKey(keyStatus)) {
            throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, "Unable to flush data to doris: unknown result status.");
        }
        if (loadResult.get(keyStatus).equals("Fail")) {
            throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, "Failed to flush data to doris.\n" + JSON.toJSONString(loadResult));
        }
    }

    /**
     * loop to get target host
     *
     * @return
     */
    private String getAvailableHost() {
        if (this.hostPos >= targetHosts.size()) {
            this.hostPos = 0;
        }

        while (this.hostPos < targetHosts.size()) {
            final String host = targetHosts.get(hostPos);
            ++this.hostPos;
            return host;
        }

        return null;
    }

    private Map<String, Object> doHttpPut(final String loadUrl, final DorisFlushBatch flushBatch) throws IOException {
        LOG.info(String.format("Executing stream load to: '%s', size: %s, rows: %d",
                loadUrl, flushBatch.getSize(), flushBatch.getRows()));

        final HttpClientBuilder httpClientBuilder = HttpClients.custom().setRedirectStrategy(new DefaultRedirectStrategy() {
            @Override
            protected boolean isRedirectable(final String method) {
                return true;
            }

            @Override
            public HttpUriRequest getRedirect(HttpRequest request, HttpResponse response, HttpContext context) throws ProtocolException {
                URI uri = this.getLocationURI(request, response, context);
                String method = request.getRequestLine().getMethod();
                if (method.equalsIgnoreCase("HEAD")) {
                    return new HttpHead(uri);
                } else if (method.equalsIgnoreCase("GET")) {
                    return new HttpGet(uri);
                } else {
                    int status = response.getStatusLine().getStatusCode();
                    return (HttpUriRequest) (status == 307 ? RequestBuilder.copy(request).setUri(uri).build() : new HttpGet(uri));
                }
            }
        });

        try (final CloseableHttpClient httpclient = httpClientBuilder.build()) {
            final HttpPut httpPut = new HttpPut(loadUrl);
            final List<String> cols = this.keys.getColumns();
            if (null != cols && !cols.isEmpty()) {
                httpPut.setHeader("columns", String.join(",", cols.stream().map(item -> String.format("`%s`", item.trim().replace("`", ""))).collect(Collectors.toList())));
            }

            //set default header
            setDefaultHeader(httpPut);
            // put custom loadProps to http header
            final Map<String, Object> loadProps = this.keys.getLoadProps();
            if (null != loadProps) {
                for (final Map.Entry<String, Object> entry : loadProps.entrySet()) {
                    httpPut.setHeader(entry.getKey(), String.valueOf(entry.getValue()));
                }
            }

            // set other required headers
            httpPut.setHeader(HttpHeaders.EXPECT, "100-continue");
            httpPut.setHeader(HttpHeaders.AUTHORIZATION, this.getBasicAuthHeader(this.keys.getUsername(), this.keys.getPassword()));
            httpPut.setHeader("label", flushBatch.getLabel());

            // Use ByteArrayEntity instead of StringEntity to handle Chinese correctly
            httpPut.setEntity(new ByteArrayEntity(flushBatch.getData().getBytes()));
            httpPut.setConfig(requestConfig);

            try (final CloseableHttpResponse resp = httpclient.execute(httpPut)) {
                final int code = resp.getStatusLine().getStatusCode();
                if (HttpStatus.SC_OK != code) {
                    LOG.warn("Request failed with code:{}", code);
                    return null;
                }
                final HttpEntity respEntity = resp.getEntity();
                if (null == respEntity) {
                    LOG.warn("Request failed with empty response.");
                    return null;
                }
                return (Map<String, Object>) JSON.parse(EntityUtils.toString(respEntity));
            }
        }
    }

    /**
     * Set default request headers in json and csv formats.
     * csv default delimiters are \x01 and \x02
     */
    private void setDefaultHeader(HttpPut httpPut) {
        if (Key.DEFAULT_FORMAT_CSV.equalsIgnoreCase(this.keys.getFormat())) {
            httpPut.setHeader("line_delimiter", this.keys.getLineDelimiter());
            httpPut.setHeader("column_separator", this.keys.getColumnSeparator());
        } else {
            httpPut.setHeader("format", "json");
            httpPut.setHeader("strip_outer_array", "true");
            httpPut.setHeader("fuzzy_parse", "true");
        }
    }

    private String getBasicAuthHeader(final String username, final String password) {
        final String auth = username + ":" + password;
        final byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes());
        return "Basic " + new String(encodedAuth);
    }

    // for test
    public static void main(String[] args) throws IOException {
        String json = "{\n" +
                "                        \"beLoadUrl\": [\"127.0.0.1:8040\"],\n" +
                "                        \"column\": [\"name\", \"age\", \"cdate\", \"cdatetime\"],\n" +
                "                        \"database\": \"test\",\n" +
                "                        \"jdbcUrl\": \"jdbc:mysql://127.0.0.1:9030/\",\n" +
                "                        \"loadProps\": {\n" +
//                "                        \"line_delimiter\": \"\\\\x03\",\n" +
//                "                        \"column_separator\": \"\\\\x04\",\n" +
                "                        },\n" +
                "                        \"format\": \"csv\",\n" +
                "                        \"password\": \"\",\n" +
                "                        \"postSql\": [],\n" +
                "                        \"preSql\": [],\n" +
                "                        \"table\": \"test_datax\",\n" +
                "                        \"maxRetries\": \"0\",\n" +
                "                        \"username\": \"root\"\n" +
                "                    }";
        Configuration configuration = Configuration.from(json);
        Key key = new Key(configuration);

        DorisWriterEmitter emitter = new DorisWriterEmitter(key);
        DorisFlushBatch flushBatch = new DorisFlushBatch(key.getLineDelimiter(), key.getFormat());

        Map<String, String> row1 = Maps.newHashMap();
        row1.put("cdate", "2021-02-02");
        row1.put("cdatetime", "2021-02-02 00:00:00");
        row1.put("name", "zhangsan");
        row1.put("age", "18");
        Map<String, String> row2 = Maps.newHashMap();
        row2.put("cdate", "2022-02-02");
        row2.put("cdatetime", "2022-02-02 10:00:00");
        row2.put("name", "lisi");
        row2.put("age", "180");
        String rowStr1 = JSON.toJSONString(row1);
        String rowStr2 = JSON.toJSONString(row2);
        if ("csv".equals(key.getFormat())) {
            rowStr1 = String.join(EscapeHandler.escapeString(key.getColumnSeparator()), "2021-02-02", "2021-02-02 00:00:00", "zhangsan", "18");
            rowStr2 = String.join(EscapeHandler.escapeString(key.getColumnSeparator()), "2022-02-02", "2022-02-02 10:00:00", "lisi", "180");
        }
        System.out.println("rows1: " + rowStr1);
        System.out.println("rows2: " + rowStr2);

        for (int i = 0; i < 1; ++i) {
            flushBatch.putData(rowStr1);
            flushBatch.putData(rowStr2);
        }
        emitter.emit(flushBatch);
    }
}
