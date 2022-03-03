package com.alibaba.datax.plugin.reader.oceanbasev10reader.util;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;

import com.alibaba.datax.common.element.Record;

public class TaskContext {
    private Connection conn;
    private final String table;
    private String indexName;
    // 辅助索引的字段列表
    private List<String> secondaryIndexColumns = Collections.emptyList();
    private String querySql;
    private final String where;
    private final int fetchSize;
    private long readBatchSize = -1;
    private boolean weakRead = true;
    private String userSavePoint;
    private String compatibleMode = ObReaderUtils.OB_COMPATIBLE_MODE_MYSQL;

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    private String partitionName;

    // 断点续读的保存点
    private volatile Record savePoint;

    // pk在column中的index,用于绑定变量时从savePoint中读取值
    // 如果这个值为null,则表示 不是断点续读的场景
    private int[] pkIndexs;

    private final List<String> columns;

    private String[] pkColumns;

    private long cost;

    private final int transferColumnNumber;

    public TaskContext(String table, List<String> columns, String where, int fetchSize) {
        super();
        this.table = table;
        this.columns = columns;
        // 针对只有querySql的场景
        this.transferColumnNumber = columns == null ? -1 : columns.size();
        this.where = where;
        this.fetchSize = fetchSize;
    }

    public Connection getConn() {
        return conn;
    }

    public void setConn(Connection conn) {
        this.conn = conn;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public List<String> getSecondaryIndexColumns() {
        return secondaryIndexColumns;
    }

    public void setSecondaryIndexColumns(List<String> secondaryIndexColumns) {
        this.secondaryIndexColumns = secondaryIndexColumns;
    }

    public String getQuerySql() {
        if (readBatchSize == -1 || ObReaderUtils.isOracleMode(compatibleMode)) {
            return querySql;
        } else {
            return querySql + " limit " + readBatchSize;
        }
    }

    public void setQuerySql(String querySql) {
        this.querySql = querySql;
    }

    public String getWhere() {
        return where;
    }

    public Record getSavePoint() {
        return savePoint;
    }

    public void setSavePoint(Record savePoint) {
        this.savePoint = savePoint;
    }

    public int[] getPkIndexs() {
        return pkIndexs;
    }

    public void setPkIndexs(int[] pkIndexs) {
        this.pkIndexs = pkIndexs;
    }

    public List<String> getColumns() {
        return columns;
    }

    public String[] getPkColumns() {
        return pkColumns;
    }

    public void setPkColumns(String[] pkColumns) {
        this.pkColumns = pkColumns;
    }

    public String getTable() {
        return table;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public long getCost() {
        return cost;
    }

    public void addCost(long cost) {
        this.cost += cost;
    }

    public int getTransferColumnNumber() {
        return transferColumnNumber;
    }

    public long getReadBatchSize() {
        return readBatchSize;
    }

    public void setReadBatchSize(long readBatchSize) {
        this.readBatchSize = readBatchSize;
    }

    public boolean getWeakRead() {
        return weakRead;
    }

    public void setWeakRead(boolean weakRead) {
        this.weakRead = weakRead;
    }

    public String getUserSavePoint() {
        return userSavePoint;
    }

    public void setUserSavePoint(String userSavePoint) {
        this.userSavePoint = userSavePoint;
    }

    public String getCompatibleMode() {
        return compatibleMode;
    }

    public void setCompatibleMode(String compatibleMode) {
        this.compatibleMode = compatibleMode;
    }
}
