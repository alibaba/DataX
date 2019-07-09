package com.alibaba.datax.plugin.reader.tablestorereader.model;

import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;

import java.util.List;

public class TableStoreConf {
    private String endpoint= null;
    private String accessId = null;
    private String accesskey = null;
    private String instanceName = null;
    private String tableName = null;
    private String indexName = null;

    private List<PrimaryKeyValue> rangeBegin = null;
    private List<PrimaryKeyValue> rangeEnd = null;
    private List<PrimaryKeyValue> rangeSplit = null;
   
    private List<TableStoreColumn> columns = null;

    private List<String> columnNames = null;

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

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public List<TableStoreColumn> getColumns() {
        return columns;
    }
    public void setColumns(List<TableStoreColumn> columns) {
        this.columns = columns;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
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
