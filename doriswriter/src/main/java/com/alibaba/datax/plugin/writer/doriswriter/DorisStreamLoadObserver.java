package com.alibaba.datax.plugin.writer.doriswriter;

import com.alibaba.fastjson2.JSON;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class DorisStreamLoadObserver implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoadObserver.class);

    private static final String RESULT_FAILED = "Fail";
    private static final String RESULT_LABEL_EXISTED = "Label Already Exists";
    private static final String LABEL_STATE_VISIBLE = "VISIBLE";
    private static final String LABEL_STATE_COMMITTED = "COMMITTED";
    private static final String RESULT_LABEL_PREPARE = "PREPARE";
    private static final String RESULT_LABEL_ABORTED = "ABORTED";
    private static final String RESULT_LABEL_UNKNOWN = "UNKNOWN";

    private final Keys options;
    private final String basicAuthHeader;
    private final List<String> hosts;
    private final AtomicInteger hostIndex = new AtomicInteger(0);
    private final Map<String, Long> hostBlacklistUntil = new ConcurrentHashMap<String, Long>();
    private final RequestConfig requestConfig;
    private final CloseableHttpClient httpClient;

    public DorisStreamLoadObserver(Keys options) {
        this.options = options;
        this.basicAuthHeader = buildBasicAuthHeader(options.getUsername(), options.getPassword());
        this.hosts = new ArrayList<String>();
        for (String host : options.getLoadUrlList()) {
            if (host.startsWith("http://") || host.startsWith("https://")) {
                this.hosts.add(host);
            } else {
                this.hosts.add("http://" + host);
            }
        }

        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setDefaultMaxPerRoute(Math.max(8, options.getFlushWorkerCount() * 4));
        connManager.setMaxTotal(Math.max(16, options.getFlushWorkerCount() * 8));

        this.requestConfig = RequestConfig.custom()
                .setConnectTimeout(options.getConnectTimeout())
                .setSocketTimeout(options.getSocketTimeout())
                .setConnectionRequestTimeout(options.getConnectionRequestTimeout())
                .setRedirectsEnabled(true)
                .build();

        HttpClientBuilder httpClientBuilder = HttpClients.custom()
                .setConnectionManager(connManager)
                .setDefaultRequestConfig(requestConfig)
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                })
                .disableAutomaticRetries();
        this.httpClient = httpClientBuilder.build();
    }

    public void streamLoad(WriterTuple data) throws Exception {
        String host = getLoadHost();
        if (host == null) {
            throw new IOException("loadUrl cannot be empty, or no Doris FE/BE host is currently available.");
        }
        String loadUrl = host + "/api/" + options.getDatabase() + "/" + options.getTable() + "/_stream_load";
        Map<String, Object> loadResult = put(loadUrl, data);
        LOG.info("StreamLoad response: {}", JSON.toJSONString(loadResult));
        final String keyStatus = "Status";
        if (loadResult == null || !loadResult.containsKey(keyStatus)) {
            markHostFailure(host);
            throw new IOException("Unable to flush data to Doris: unknown result status.");
        }
        Object status = loadResult.get(keyStatus);
        if (RESULT_FAILED.equals(status)) {
            markHostFailure(host);
            throw new DorisWriterExcetion("Failed to flush data to Doris. " + JSON.toJSONString(loadResult), loadResult);
        }
        if (RESULT_LABEL_EXISTED.equals(status)) {
            checkStreamLoadState(host, data.getLabel());
        } else {
            markHostSuccess(host);
        }
    }

    private void checkStreamLoadState(String host, String label) throws IOException {
        int idx = 0;
        while (true) {
            try {
                Thread.sleep(1000L * Math.min(++idx, 5));
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while checking stream load label state.", ex);
            }

            HttpGet httpGet = new HttpGet(host + "/api/" + options.getDatabase() + "/get_load_state?label=" + label);
            httpGet.setHeader("Authorization", basicAuthHeader);
            httpGet.setConfig(requestConfig);
            try (CloseableHttpResponse resp = httpClient.execute(httpGet)) {
                HttpEntity respEntity = getHttpEntity(resp);
                if (respEntity == null) {
                    throw new IOException(String.format("Failed to get final state for label[%s].", label));
                }
                Map<String, Object> result = (Map<String, Object>) JSON.parse(EntityUtils.toString(respEntity, StandardCharsets.UTF_8));
                String labelState = result == null ? null : (String) result.get("data");
                if (labelState == null) {
                    throw new IOException(String.format("Failed to get final state of label[%s]. response[%s]", label, JSON.toJSONString(result)));
                }
                LOG.info("Checking label[{}] state[{}]", label, labelState);
                switch (labelState) {
                    case LABEL_STATE_VISIBLE:
                    case LABEL_STATE_COMMITTED:
                        markHostSuccess(host);
                        return;
                    case RESULT_LABEL_PREPARE:
                        continue;
                    case RESULT_LABEL_ABORTED:
                        markHostFailure(host);
                        throw new DorisWriterExcetion(String.format("Failed to flush data to Doris, label[%s] state[%s]", label, labelState), result, true);
                    case RESULT_LABEL_UNKNOWN:
                    default:
                        markHostFailure(host);
                        throw new IOException(String.format("Failed to flush data to Doris, label[%s] state[%s]", label, labelState));
                }
            }
        }
    }

    private Map<String, Object> put(String loadUrl, WriterTuple data) throws IOException {
        LOG.info("Executing stream load: url='{}', rows={}, size={}, label={}", loadUrl, data.getRows(), data.getBytes(), data.getLabel());
        HttpPut httpPut = new HttpPut(loadUrl);
        httpPut.removeHeaders(HttpHeaders.CONTENT_LENGTH);
        httpPut.removeHeaders(HttpHeaders.TRANSFER_ENCODING);
        if (Keys.StreamLoadFormat.CSV.equals(options.getStreamLoadFormat())) {
            List<String> cols = options.getColumns();
            if (cols != null && !cols.isEmpty()) {
                httpPut.setHeader("columns", String.join(",", cols.stream().map(f -> String.format("`%s`", f)).collect(Collectors.toList())));
            }
        }
        Map<String, Object> loadProps = options.getLoadProps();
        if (loadProps != null) {
            for (Map.Entry<String, Object> entry : loadProps.entrySet()) {
                httpPut.setHeader(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        httpPut.setHeader("Expect", "100-continue");
        httpPut.setHeader("label", data.getLabel());
        httpPut.setHeader("two_phase_commit", "false");
        httpPut.setHeader("Authorization", basicAuthHeader);
        httpPut.setEntity(new ByteArrayEntity(data.getData()));
        httpPut.setConfig(requestConfig);
        try (CloseableHttpResponse resp = httpClient.execute(httpPut)) {
            HttpEntity respEntity = getHttpEntity(resp);
            if (respEntity == null) {
                return null;
            }
            return (Map<String, Object>) JSON.parse(EntityUtils.toString(respEntity, StandardCharsets.UTF_8));
        }
    }

    private HttpEntity getHttpEntity(CloseableHttpResponse resp) throws IOException {
        int code = resp.getStatusLine().getStatusCode();
        if (code < 200 || code >= 300) {
            String responseText = resp.getEntity() == null ? "" : EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8);
            throw new IOException("Request failed with code=" + code + ", response=" + responseText);
        }
        HttpEntity respEntity = resp.getEntity();
        if (respEntity == null) {
            LOG.warn("Request succeeded but response body is empty.");
            return null;
        }
        return respEntity;
    }

    private String getLoadHost() {
        if (hosts.isEmpty()) {
            return null;
        }
        long now = System.currentTimeMillis();
        int size = hosts.size();
        int start = Math.abs(hostIndex.getAndIncrement());
        for (int i = 0; i < size; i++) {
            String host = hosts.get((start + i) % size);
            Long blockedUntil = hostBlacklistUntil.get(host);
            if (blockedUntil == null || blockedUntil <= now) {
                return host;
            }
        }
        String fallback = hosts.get(start % size);
        LOG.warn("All Doris hosts are in cooldown; fallback to host {}", fallback);
        return fallback;
    }

    private void markHostFailure(String host) {
        hostBlacklistUntil.put(host, System.currentTimeMillis() + options.getHostCooldownMs());
    }

    private void markHostSuccess(String host) {
        hostBlacklistUntil.remove(host);
    }

    private String buildBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encodedAuth, StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }
}
