package com.alibaba.datax.plugin.reader.tsdbreader.conn;

import java.util.List;
import java.util.Map;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：Query Result
 *
 * @author Benedict Jin
 * @since 2019-09-19
 */
public class QueryResult {

    private String metricName;
    private Map<String, Object> tags;
    private List<String> groupByTags;
    private List<String> aggregatedTags;
    private Map<String, Object> dps;

    public QueryResult() {
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public Map<String, Object> getTags() {
        return tags;
    }

    public void setTags(Map<String, Object> tags) {
        this.tags = tags;
    }

    public List<String> getGroupByTags() {
        return groupByTags;
    }

    public void setGroupByTags(List<String> groupByTags) {
        this.groupByTags = groupByTags;
    }

    public List<String> getAggregatedTags() {
        return aggregatedTags;
    }

    public void setAggregatedTags(List<String> aggregatedTags) {
        this.aggregatedTags = aggregatedTags;
    }

    public Map<String, Object> getDps() {
        return dps;
    }

    public void setDps(Map<String, Object> dps) {
        this.dps = dps;
    }
}
