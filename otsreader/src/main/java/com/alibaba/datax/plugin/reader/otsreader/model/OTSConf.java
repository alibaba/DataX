package com.alibaba.datax.plugin.reader.otsreader.model;

import java.util.List;

import com.aliyun.openservices.ots.model.PrimaryKeyValue;

public class OTSConf {
    private String endpoint= null;
    private String accessId = null;
    private String accesskey = null;
    private String instanceName = null;
    private String tableName = null;
    
    private List<PrimaryKeyValue> rangeBegin = null;
    private List<PrimaryKeyValue> rangeEnd = null;
    private List<PrimaryKeyValue> rangeSplit = null;
   
    private List<OTSColumn> columns = null;
    
    private int retry;
    private int sleepInMilliSecond;
    
    public String getEndpoint() {
        return endpoint;
    }
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }
    public String getAccessId() {
        return accessId;
    }
    public void setAccessId(String accessId) {
        this.accessId = accessId;
    }
    public String getAccesskey() {
        return accesskey;
    }
    public void setAccesskey(String accesskey) {
        this.accesskey = accesskey;
    }
    public String getInstanceName() {
        return instanceName;
    }
    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }
    public String getTableName() {
        return tableName;
    }
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<OTSColumn> getColumns() {
        return columns;
    }
    public void setColumns(List<OTSColumn> columns) {
        this.columns = columns;
    }
    public int getRetry() {
        return retry;
    }
    public void setRetry(int retry) {
        this.retry = retry;
    }
    public int getSleepInMilliSecond() {
        return sleepInMilliSecond;
    }
    public void setSleepInMilliSecond(int sleepInMilliSecond) {
        this.sleepInMilliSecond = sleepInMilliSecond;
    }
    public List<PrimaryKeyValue> getRangeBegin() {
        return rangeBegin;
    }
    public void setRangeBegin(List<PrimaryKeyValue> rangeBegin) {
        this.rangeBegin = rangeBegin;
    }
    public List<PrimaryKeyValue> getRangeEnd() {
        return rangeEnd;
    }
    public void setRangeEnd(List<PrimaryKeyValue> rangeEnd) {
        this.rangeEnd = rangeEnd;
    }
    public List<PrimaryKeyValue> getRangeSplit() {
        return rangeSplit;
    }
    public void setRangeSplit(List<PrimaryKeyValue> rangeSplit) {
        this.rangeSplit = rangeSplit;
    }
}
