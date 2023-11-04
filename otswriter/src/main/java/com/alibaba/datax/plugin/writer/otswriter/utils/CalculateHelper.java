package com.alibaba.datax.plugin.writer.otswriter.utils;

import com.alibaba.datax.plugin.writer.otswriter.OTSCriticalException;
import com.alicloud.openservices.tablestore.core.utils.Pair;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesKey;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesRow;

import java.util.List;
import java.util.Map;

import static com.alicloud.openservices.tablestore.model.PrimaryKeyValue.AUTO_INCREMENT;

public class CalculateHelper {
    private static int getPrimaryKeyValueSize(PrimaryKeyValue primaryKeyValue) throws OTSCriticalException {
        int primaryKeySize = 0;
        if(primaryKeyValue == AUTO_INCREMENT){
            return primaryKeySize;
        }
        switch (primaryKeyValue.getType()) {
            case INTEGER:
                primaryKeySize = 8;
                break;
            case STRING:
                primaryKeySize = primaryKeyValue.asStringInBytes().length;
                break;
            case BINARY:
                primaryKeySize = primaryKeyValue.asBinary().length;
                break;
            default:
                throw new OTSCriticalException("Bug: not support the type : " + primaryKeyValue.getType() + " in getPrimaryKeyValueSize");
        }
        return primaryKeySize;
    }
    
    private static int getColumnValueSize(ColumnValue columnValue) throws OTSCriticalException {
        int columnSize = 0;
        switch (columnValue.getType()) {
            case INTEGER:
                columnSize += 8;
                break;
            case DOUBLE:
                columnSize += 8; 
                break;
            case STRING:
                columnSize += columnValue.asStringInBytes().length;
                break;
            case BINARY:
                columnSize += columnValue.asBinary().length;
                break;
            case BOOLEAN:
                columnSize += 1; 
                break;
            default:
                throw new OTSCriticalException("Bug: not support the type : " + columnValue.getType() + " in getColumnValueSize");
            }
        return columnSize;
    }
    
    public static int getRowPutChangeSize(RowPutChange change) throws OTSCriticalException {
        int primaryKeyTotalSize = 0;
        int columnTotalSize = 0;
        
        // PrimaryKeys Total Size
        PrimaryKey primaryKey = change.getPrimaryKey();
        PrimaryKeyColumn[] primaryKeyColumnArray = primaryKey.getPrimaryKeyColumns();
        PrimaryKeyColumn primaryKeyColumn;
        byte[] primaryKeyName;
        PrimaryKeyValue primaryKeyValue;
        for (int i = 0; i < primaryKeyColumnArray.length; i++) {
            primaryKeyColumn = primaryKeyColumnArray[i];
            primaryKeyName = primaryKeyColumn.getNameRawData();
            primaryKeyValue = primaryKeyColumn.getValue();
            
            // += PrimaryKey Name Data
            primaryKeyTotalSize += primaryKeyName.length;
            
            // += PrimaryKey Value Data
            primaryKeyTotalSize += getPrimaryKeyValueSize(primaryKeyValue);
        }
        
        // Columns Total Size
        List<Column> columnList = change.getColumnsToPut();
        for (Column column : columnList) {
            // += Column Name
            columnTotalSize += column.getNameRawData().length; 
            
            // += Column Value
            ColumnValue columnValue = column.getValue();
            
            columnTotalSize += getColumnValueSize(columnValue);
            
            // += Timestamp
            if (column.hasSetTimestamp()) {
                columnTotalSize += 8;
            }   
        }
        
        return primaryKeyTotalSize + columnTotalSize; 
    }

    public static int getRowUpdateChangeSize(RowUpdateChange change) throws OTSCriticalException {
        int primaryKeyTotalSize = 0;
        int columnPutSize = 0;
        int columnDeleteSize = 0;
        
        // PrimaryKeys Total Size
        PrimaryKey primaryKey = change.getPrimaryKey();
        PrimaryKeyColumn[] primaryKeyColumnArray = primaryKey.getPrimaryKeyColumns();
        PrimaryKeyColumn primaryKeyColumn;
        byte[] primaryKeyName;
        PrimaryKeyValue primaryKeyValue;
        for (int i = 0; i < primaryKeyColumnArray.length; i++) {
            primaryKeyColumn = primaryKeyColumnArray[i];
            primaryKeyName = primaryKeyColumn.getNameRawData();
            primaryKeyValue = primaryKeyColumn.getValue();
            
            // += PrimaryKey Name Data
            primaryKeyTotalSize += primaryKeyName.length;
            
            // += PrimaryKey Value Data
            primaryKeyTotalSize += getPrimaryKeyValueSize(primaryKeyValue);
        }
        
        // Column Total Size 
        List<Pair<Column,RowUpdateChange.Type>> updatePairList = change.getColumnsToUpdate();
        Column column;
        ColumnValue columnValue;
        RowUpdateChange.Type type;
        for (Pair<Column,RowUpdateChange.Type> updatePair : updatePairList) {
            column = updatePair.getFirst();
            type = updatePair.getSecond();
            
            switch (type) {
            case DELETE:
                columnDeleteSize += column.getNameRawData().length;
                columnDeleteSize += 8;// Timestamp
                break;
            case DELETE_ALL:
                columnDeleteSize += column.getNameRawData().length;
                break;
            case PUT:
                // Name
                columnPutSize += column.getNameRawData().length;
                
                // Value
                columnValue = column.getValue();
                columnPutSize += getColumnValueSize(columnValue);
                break;
            default:
                throw new OTSCriticalException("Bug: not support the type : " + type);
            }
        }
        
        return primaryKeyTotalSize + columnPutSize + columnDeleteSize;
    }

    public static int getTimeseriesRowDataSize(TimeseriesRow row) {
        TimeseriesKey timeseriesKey = row.getTimeseriesKey();
        Map<String, ColumnValue> fields =  row.getFields();
        int totalSize = 0;
        totalSize += 8;     // time size
        totalSize += com.alicloud.openservices.tablestore.core.utils.CalculateHelper.calcStringSizeInBytes(timeseriesKey.getMeasurementName());
        totalSize += com.alicloud.openservices.tablestore.core.utils.CalculateHelper.calcStringSizeInBytes(timeseriesKey.getDataSource());
        totalSize += com.alicloud.openservices.tablestore.core.utils.CalculateHelper.calcStringSizeInBytes(timeseriesKey.buildTagsString());
        for (Map.Entry<String, ColumnValue> entry : fields.entrySet()) {
            totalSize += entry.getValue().getDataSize() + com.alicloud.openservices.tablestore.core.utils.CalculateHelper.calcStringSizeInBytes(entry.getKey());
        }
        return totalSize;
    }
}
