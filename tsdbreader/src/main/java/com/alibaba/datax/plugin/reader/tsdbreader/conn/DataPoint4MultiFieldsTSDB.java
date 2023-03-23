package com.alibaba.datax.plugin.reader.tsdbreader.conn;

import com.alibaba.fastjson2.JSON;

import java.util.Map;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：DataPoint for TSDB with Multi Fields
 *
 * @author Benedict Jin
 * @since 2019-10-21
 */
public class DataPoint4MultiFieldsTSDB {

    private long timestamp;
    private String metric;
    private Map<String, Object> tags;
    private Map<String, Object> fields;

    public DataPoint4MultiFieldsTSDB() {
    }

    public DataPoint4MultiFieldsTSDB(long timestamp, String metric, Map<String, Object> tags, Map<String, Object> fields) {
        this.timestamp = timestamp;
        this.metric = metric;
        this.tags = tags;
        this.fields = fields;
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

    public Map<String, Object> getTags() {
        return tags;
    }

    public void setTags(Map<String, Object> tags) {
        this.tags = tags;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
