package com.dorisdb.connector.datax.plugin.writer.doriswriter.manager;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import com.alibaba.fastjson.JSON;
import com.dorisdb.connector.datax.plugin.writer.doriswriter.DorisWriterOptions;
import com.dorisdb.connector.datax.plugin.writer.doriswriter.row.DorisDelimiterParser;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

 
public class DorisStreamLoadVisitor {

    private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoadVisitor.class);

    private final DorisWriterOptions writerOptions;
    private int pos;

    public DorisStreamLoadVisitor(DorisWriterOptions writerOptions) {
        this.writerOptions = writerOptions;
    }

    public void doStreamLoad(DorisFlushTuple flushData) throws IOException {
        String host = getAvailableHost();
        if (null == host) {
            throw new IOException("None of the host in `load_url` could be connected.");
        }
        String loadUrl = new StringBuilder(host)
            .append("/api/")
            .append(writerOptions.getDatabase())
            .append("/")
            .append(writerOptions.getTable())
            .append("/_stream_load")
            .toString();
        LOG.debug(String.format("Start to join batch data: rows[%d] bytes[%d] label[%s].", flushData.getRows().size(), flushData.getBytes(), flushData.getLabel()));
        Map<String, Object> loadResult = doHttpPut(loadUrl, flushData.getLabel(), joinRows(flushData.getRows()));
        final String keyStatus = "Status";
        if (null == loadResult || !loadResult.containsKey(keyStatus)) {
            throw new IOException("Unable to flush data to doris: unknown result status.");
        }
        LOG.debug(new StringBuilder("StreamLoad response:\n").append(JSON.toJSONString(loadResult)).toString());
        if (loadResult.get(keyStatus).equals("Fail")) {
            throw new IOException(
                new StringBuilder("Failed to flush data to doris.\n").append(JSON.toJSONString(loadResult)).toString()
            );
        }
    }

    private String getAvailableHost() {
        List<String> hostList = writerOptions.getLoadUrlList();
        if (pos >= hostList.size()) {
            pos = 0;
        }
        for (; pos < hostList.size(); pos++) {
            String host = new StringBuilder("http://").append(hostList.get(pos)).toString();
            if (tryHttpConnection(host)) {
                return host;
            }
        }
        return null;
    }

    private boolean tryHttpConnection(String host) {
        try {  
            URL url = new URL(host);
            HttpURLConnection co =  (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(1000);
            co.connect();
            co.disconnect();
            return true;
        } catch (Exception e1) {
            LOG.warn("Failed to connect to address:{}", host, e1);
            return false;
        }
    }

    private byte[] joinRows(List<String> rows) {
        if (DorisWriterOptions.StreamLoadFormat.CSV.equals(writerOptions.getStreamLoadFormat())) {
            Map<String, Object> props = writerOptions.getLoadProps();
            String lineDelimiter = "\n";
            if (null != props && props.containsKey("row_delimiter")) {
                lineDelimiter = DorisDelimiterParser.parse(String.valueOf(props.get("row_delimiter")), "\n");
            }
            return (String.join(lineDelimiter, rows) + lineDelimiter).getBytes(StandardCharsets.UTF_8);
        }
        if (DorisWriterOptions.StreamLoadFormat.JSON.equals(writerOptions.getStreamLoadFormat())) {
            return new StringBuilder("[").append(String.join(",", rows)).append("]").toString().getBytes(StandardCharsets.UTF_8);
        }
        throw new RuntimeException("Failed to join rows data, unsupported `format` from stream load properties:");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> doHttpPut(String loadUrl, String label, byte[] data) throws IOException {
        LOG.info(String.format("Executing stream load to: '%s', size: '%s'", loadUrl, data.length));
        final HttpClientBuilder httpClientBuilder = HttpClients.custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    return true;
                }
            });
        try (CloseableHttpClient httpclient = httpClientBuilder.build()) {
            HttpPut httpPut = new HttpPut(loadUrl);
            List<String> cols = writerOptions.getColumns();
            if (null != cols && !cols.isEmpty()) {
                httpPut.setHeader("columns", String.join(",", cols));
            }
            if (null != writerOptions.getLoadProps()) {
                for (Map.Entry<String, Object> entry : writerOptions.getLoadProps().entrySet()) {
                    httpPut.setHeader(entry.getKey(), String.valueOf(entry.getValue()));
                }
            }
            httpPut.setHeader("Expect", "100-continue");
            httpPut.setHeader("label", label);
            httpPut.setHeader("Content-Type", "application/x-www-form-urlencoded");
            httpPut.setHeader("Authorization", getBasicAuthHeader(writerOptions.getUsername(), writerOptions.getPassword()));
            httpPut.setEntity(new ByteArrayEntity(data));
            httpPut.setConfig(RequestConfig.custom().setRedirectsEnabled(true).build());
            try (CloseableHttpResponse resp = httpclient.execute(httpPut)) {
                int code = resp.getStatusLine().getStatusCode();
                if (200 != code) {
                    LOG.warn("Request failed with code:{}", code);
                    return null;
                }
                HttpEntity respEntity = resp.getEntity();
                if (null == respEntity) {
                    LOG.warn("Request failed with empty response.");
                    return null;
                }
                return (Map<String, Object>)JSON.parse(EntityUtils.toString(respEntity));
            }
        }
    }
    
    private String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes());
        return new StringBuilder("Basic ").append(new String(encodedAuth)).toString();
    }

}
