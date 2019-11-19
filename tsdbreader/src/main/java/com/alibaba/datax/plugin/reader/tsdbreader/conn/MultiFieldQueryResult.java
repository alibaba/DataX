package com.alibaba.datax.plugin.reader.tsdbreader.conn;

import java.util.List;
import java.util.Map;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：Multi Field Query Result
 *
 * @author Benedict Jin
 * @since 2019-10-22
 */
public class MultiFieldQueryResult {

    private String metric;
    private Map<String, Object> tags;
    private List<String> aggregatedTags;
    private List<String> columns;
    private List<List<Object>> values;

    public MultiFieldQueryResult() {
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

    public List<String> getAggregatedTags() {
        return aggregatedTags;
    }

    public void setAggregatedTags(List<String> aggregatedTags) {
        this.aggregatedTags = aggregatedTags;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<List<Object>> getValues() {
        return values;
    }

    public void setValues(List<List<Object>> values) {
        this.values = values;
    }
}
