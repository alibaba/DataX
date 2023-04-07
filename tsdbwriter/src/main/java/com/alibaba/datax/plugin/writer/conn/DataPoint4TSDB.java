package com.alibaba.datax.plugin.writer.conn;

import com.alibaba.fastjson2.JSON;

import java.util.Map;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：DataPoint for TSDB
 *
 * @author Benedict Jin
 * @since 2019-04-10
 */
public class DataPoint4TSDB {

    private long timestamp;
    private String metric;
    private Map<String, String> tags;
    private Object value;

    public DataPoint4TSDB() {
    }

    public DataPoint4TSDB(long timestamp, String metric, Map<String, String> tags, Object value) {
        this.timestamp = timestamp;
        this.metric = metric;
        this.tags = tags;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
