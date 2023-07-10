package com.alibaba.datax.plugin.writer.cnosdbwriter.format.opentsdb;

import com.alibaba.fastjson2.JSON;

import java.util.Map;

/**
 * Serializer&Deserializer of com.alibaba.datax.plugin.reader.conn.DataPoint4TSDB
 */
public class CnosDBOpenTSDBPoint {
    private long timestamp;
    private String metric;
    private Map<String, String> tags;
    private Object value;

    public CnosDBOpenTSDBPoint() {
    }

    public CnosDBOpenTSDBPoint(long timestamp, String metric, Map<String, String> tags, Object value) {
        this.timestamp = timestamp;
        this.metric = metric;
        this.tags = tags;
        this.value = value;
    }

    public static CnosDBOpenTSDBPoint fromJSON(String json) {
        return JSON.parseObject(json, CnosDBOpenTSDBPoint.class);
    }

    public String toJSON() {
        return JSON.toJSONString(this);
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
}
