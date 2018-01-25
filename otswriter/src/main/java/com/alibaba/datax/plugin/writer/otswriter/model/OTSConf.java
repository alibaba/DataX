package com.alibaba.datax.plugin.writer.otswriter.model;

import java.util.List;

public class OTSConf {
    private String endpoint;
    private String accessId;
    private String accessKey;
    private String instanceName;
    private String tableName;
   
    private List<OTSPKColumn> primaryKeyColumn;
    private List<OTSAttrColumn> attributeColumn;

    private int bufferSize = 1024;
    private int retry =  18;
    private int sleepInMillisecond = 100;
    private int batchWriteCount = 10;
    private int concurrencyWrite = 5;
    private int ioThreadCount = 1;
    private int socketTimeout = 20000;
    private int connectTimeout = 10000;
    
    private OTSOpType operation;
    private RestrictConf restrictConf;

    //限制项
    public class RestrictConf {
        private int requestTotalSizeLimition = 1024 * 1024;
        private int primaryKeyColumnSize = 1024;
        private int attributeColumnSize = 2 * 1024 * 1024;
        private int maxColumnsCount = 1024;

        public int getRequestTotalSizeLimition() {
            return requestTotalSizeLimition;
        }
        public void setRequestTotalSizeLimition(int requestTotalSizeLimition) {
            this.requestTotalSizeLimition = requestTotalSizeLimition;
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

    public RestrictConf getRestrictConf() {
        return restrictConf;
    }
    public void setRestrictConf(RestrictConf restrictConf) {
        this.restrictConf = restrictConf;
    }
    public OTSOpType getOperation() {
        return operation;
    }
    public void setOperation(OTSOpType operation) {
        this.operation = operation;
    }
    public List<OTSPKColumn> getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }
    public void setPrimaryKeyColumn(List<OTSPKColumn> primaryKeyColumn) {
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
    public List<OTSAttrColumn> getAttributeColumn() {
        return attributeColumn;
    }
    public void setAttributeColumn(List<OTSAttrColumn> attributeColumn) {
        this.attributeColumn = attributeColumn;
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