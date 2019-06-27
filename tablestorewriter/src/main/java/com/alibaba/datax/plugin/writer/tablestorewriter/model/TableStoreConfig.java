package com.alibaba.datax.plugin.writer.tablestorewriter.model;

import java.util.List;

public class TableStoreConfig {
    private String endpoint;
    private String accessId;
    private String accessKey;
    private String instanceName;
    private String tableName;
    private String tableLogicalName;
   
    private List<TableStorePKColumn> primaryKeyColumn;
    private List<TableStoreAttrColumn> attrColumn;

    private int bufferSize = 1024;
    private int retry =  18;
    private int sleepInMillisecond = 100;
    private int batchWriteCount = 10;
    private int concurrencyWrite = 5;
    private int ioThreadCount = 1;
    private int socketTimeout = 20000;
    private int connectTimeout = 10000;
    
    private TableStoreOpType operation;
    private RestrictConfig restrictConfig;

    //限制项
    public class RestrictConfig {
        private int requestTotalSizeLimitation = 1024 * 1024;
        private int primaryKeyColumnSize = 1024;
        private int attributeColumnSize = 2 * 1024 * 1024;
        private int maxColumnsCount = 1024;

        public int getRequestTotalSizeLimitation() {
            return requestTotalSizeLimitation;
        }
        public void setRequestTotalSizeLimitation(int requestTotalSizeLimitation) {
            this.requestTotalSizeLimitation = requestTotalSizeLimitation;
        }

        public void setPrimaryKeyColumnSize(int primaryKeyColumnSize) {
            this.primaryKeyColumnSize = primaryKeyColumnSize;
        }

        public void setAttributeColumnSize(int attributeColumnSize) {
            this.attributeColumnSize = attributeColumnSize;
        }

        public void setMaxColumnsCount(int maxColumnsCount) {
            this.maxColumnsCount = maxColumnsCount;
        }

        public int getAttributeColumnSize() {
            return attributeColumnSize;
        }

        public int getMaxColumnsCount() {
            return maxColumnsCount;
        }

        public int getPrimaryKeyColumnSize() {
            return primaryKeyColumnSize;
        }
    }

    public RestrictConfig getRestrictConfig() {
        return restrictConfig;
    }
    public void setRestrictConfig(RestrictConfig restrictConfig) {
        this.restrictConfig = restrictConfig;
    }
    public TableStoreOpType getOperation() {
        return operation;
    }
    public void setOperation(TableStoreOpType operation) {
        this.operation = operation;
    }
    public List<TableStorePKColumn> getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }
    public void setPrimaryKeyColumn(List<TableStorePKColumn> primaryKeyColumn) {
        this.primaryKeyColumn = primaryKeyColumn;
    }
    
    public int getConcurrencyWrite() {
        return concurrencyWrite;
    }
    public void setConcurrencyWrite(int concurrencyWrite) {
        this.concurrencyWrite = concurrencyWrite;
    }
    public int getBatchWriteCount() {
        return batchWriteCount;
    }
    public void setBatchWriteCount(int batchWriteCount) {
        this.batchWriteCount = batchWriteCount;
    }
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
    public String getAccessKey() {
        return accessKey;
    }
    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
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
    public String getTableLogicalName() {
        return tableLogicalName;
    }
    public void setTableLogicalName(String tableLogicalName) {
        this.tableLogicalName = tableLogicalName;
    }

    public List<TableStoreAttrColumn> getAttrColumn() {
        return attrColumn;
    }
    public void setAttrColumn(List<TableStoreAttrColumn> attrColumn) {
        this.attrColumn = attrColumn;
    }
    public int getRetry() {
        return retry;
    }
    public void setRetry(int retry) {
        this.retry = retry;
    }
    public int getSleepInMillisecond() {
        return sleepInMillisecond;
    }
    public void setSleepInMillisecond(int sleepInMillisecond) {
        this.sleepInMillisecond = sleepInMillisecond;
    }
    public int getIoThreadCount() {
        return ioThreadCount;
    }
    public void setIoThreadCount(int ioThreadCount) {
        this.ioThreadCount = ioThreadCount;
    }
    public int getSocketTimeout() {
        return socketTimeout;
    }
    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }
    public int getConnectTimeout() {
        return connectTimeout;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }
}