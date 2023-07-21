package com.alibaba.datax.plugin.writer.otswriter.model;

import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class OTSConf {
    private String endpoint= null;
    private String accessId = null;
    private String accessKey = null;
    private String instanceName = null;
    private String tableName = null;

   
    private List<PrimaryKeySchema> primaryKeyColumn = null;
    private List<OTSAttrColumn> attributeColumn = null;
   
    private int retry =  -1;
    private int sleepInMillisecond = -1;
    private int batchWriteCount = -1;
    private int concurrencyWrite = -1;
    private int ioThreadCount = -1;
    private int socketTimeoutInMillisecond = -1;
    private int connectTimeoutInMillisecond = -1;
    
    private OTSOpType operation = null;

    private int requestTotalSizeLimitation = -1;
    
    private OTSMode mode = null;
    private boolean enableAutoIncrement = false;
    private boolean isNewVersion = false;
    private boolean isTimeseriesTable = false;
    private TimeUnit timeUnit = TimeUnit.MICROSECONDS;
    private long timestamp = -1;
    private Map<String, Integer> encodePkColumnMapping = null;
    private String columnNamePrefixFilter = null;

    public Map<String, Integer> getEncodePkColumnMapping() {
        return encodePkColumnMapping;
    }
    public void setEncodePkColumnMapping(Map<String, Integer> encodePkColumnMapping) {
        this.encodePkColumnMapping = encodePkColumnMapping;
    }
    public int getSocketTimeoutInMillisecond() {
        return socketTimeoutInMillisecond;
    }
    public OTSOpType getOperation() {
        return operation;
    }
    public void setOperation(OTSOpType operation) {
        this.operation = operation;
    }
    public List<PrimaryKeySchema> getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }
    public void setPrimaryKeyColumn(List<PrimaryKeySchema> primaryKeyColumn) {
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
        return socketTimeoutInMillisecond;
    }
    public void setSocketTimeoutInMillisecond(int socketTimeoutInMillisecond) {
        this.socketTimeoutInMillisecond = socketTimeoutInMillisecond;
    }
    public int getConnectTimeoutInMillisecond() {
        return connectTimeoutInMillisecond;
    }
    public void setConnectTimeoutInMillisecond(int connectTimeoutInMillisecond) {
        this.connectTimeoutInMillisecond = connectTimeoutInMillisecond;
    }
    public OTSMode getMode() {
        return mode;
    }
    public void setMode(OTSMode mode) {
        this.mode = mode;
    }
    public long getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    public String getColumnNamePrefixFilter() {
        return columnNamePrefixFilter;
    }
    public void setColumnNamePrefixFilter(String columnNamePrefixFilter) {
        this.columnNamePrefixFilter = columnNamePrefixFilter;
    }

    public boolean getEnableAutoIncrement() {
        return enableAutoIncrement;
    }

    public void setEnableAutoIncrement(boolean enableAutoIncrement) {
        this.enableAutoIncrement = enableAutoIncrement;
    }
    public boolean isNewVersion() {
        return isNewVersion;
    }

    public void setNewVersion(boolean newVersion) {
        isNewVersion = newVersion;
    }

    public boolean isTimeseriesTable() {
        return isTimeseriesTable;
    }

    public void setTimeseriesTable(boolean timeseriesTable) {
        isTimeseriesTable = timeseriesTable;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    public int getRequestTotalSizeLimitation() {
        return requestTotalSizeLimitation;
    }

    public void setRequestTotalSizeLimitation(int requestTotalSizeLimitation) {
        this.requestTotalSizeLimitation = requestTotalSizeLimitation;
    }
}