package com.alibaba.datax.plugin.writer.adswriter.ads;

import java.util.ArrayList;
import java.util.List;

/**
 * ADS table meta.<br>
 * <p>
 * select table_schema, table_name,comments <br>
 * from information_schema.tables <br>
 * where table_schema='alimama' and table_name='click_af' limit 1 <br>
 * </p>
 * <p>
 * select ordinal_position,column_name,data_type,type_name,column_comment <br>
 * from information_schema.columns <br>
 * where table_schema='db_name' and table_name='table_name' <br>
 * and is_deleted=0 <br>
 * order by ordinal_position limit 1000 <br>
 * </p>
 *
 * @since 0.0.1
 */
public class TableInfo {

    private String tableSchema;
    private String tableName;
    private List<ColumnInfo> columns;
    private String comments;
    private String tableType;

    private String updateType;
    private String partitionType;
    private String partitionColumn;
    private int partitionCount;
    private List<String> primaryKeyColumns;

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("TableInfo [tableSchema=").append(tableSchema).append(", tableName=").append(tableName)
                .append(", columns=").append(columns).append(", comments=").append(comments).append(",updateType=").append(updateType)
                .append(",partitionType=").append(partitionType).append(",partitionColumn=").append(partitionColumn).append(",partitionCount=").append(partitionCount)
                .append(",primaryKeyColumns=").append(primaryKeyColumns).append("]");
        return builder.toString();
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<ColumnInfo> getColumns() {
        return columns;
    }
    
    public List<String> getColumnsNames() {
        List<String> columnNames = new ArrayList<String>();
        for (ColumnInfo column : this.getColumns()) {
            columnNames.add(column.getName());
        }
        return columnNames;
    }

    public void setColumns(List<ColumnInfo> columns) {
        this.columns = columns;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }
    
    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public String getUpdateType() {
        return updateType;
    }

    public void setUpdateType(String updateType) {
        this.updateType = updateType;
    }

    public String getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(String partitionType) {
        this.partitionType = partitionType;
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }

    public void setPartitionColumn(String partitionColumn) {
        this.partitionColumn = partitionColumn;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    public List<String> getPrimaryKeyColumns() {
        return primaryKeyColumns;
    }

    public void setPrimaryKeyColumns(List<String> primaryKeyColumns) {
        this.primaryKeyColumns = primaryKeyColumns;
    }

}
