package com.alibaba.datax.plugin.writer.adswriter.load;

import com.alibaba.datax.plugin.writer.adswriter.ads.ColumnDataType;
import com.alibaba.datax.plugin.writer.adswriter.ads.ColumnInfo;
import com.alibaba.datax.plugin.writer.adswriter.ads.TableInfo;
import com.alibaba.datax.plugin.writer.adswriter.odps.DataType;
import com.alibaba.datax.plugin.writer.adswriter.odps.FieldSchema;
import com.alibaba.datax.plugin.writer.adswriter.odps.TableMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Table meta helper for ADS writer.
 *
 * @since 0.0.1
 */
public class TableMetaHelper {

    private TableMetaHelper() {
    }

    /**
     * Create temporary ODPS table.
     * 
     * @param tableMeta table meta
     * @param lifeCycle for temporary table
     * @return ODPS temporary table meta
     */
    public static TableMeta createTempODPSTable(TableInfo tableMeta, int lifeCycle) {
        TableMeta tempTable = new TableMeta();
        tempTable.setComment(tableMeta.getComments());
        tempTable.setLifeCycle(lifeCycle);
        String tableSchema = tableMeta.getTableSchema();
        String tableName = tableMeta.getTableName();
        tempTable.setTableName(generateTempTableName(tableSchema, tableName));
        List<FieldSchema> tempColumns = new ArrayList<FieldSchema>();
        List<ColumnInfo> columns = tableMeta.getColumns();
        for (ColumnInfo column : columns) {
            FieldSchema tempColumn = new FieldSchema();
            tempColumn.setName(column.getName());
            tempColumn.setType(toODPSDataType(column.getDataType()));
            tempColumn.setComment(column.getComment());
            tempColumns.add(tempColumn);
        }
        tempTable.setCols(tempColumns);
        tempTable.setPartitionKeys(null);
        return tempTable;
    }

    private static String toODPSDataType(ColumnDataType columnDataType) {
        int type;
        switch (columnDataType.type) {
            case ColumnDataType.BOOLEAN:
                type = DataType.STRING;
                break;
            case ColumnDataType.BYTE:
            case ColumnDataType.SHORT:
            case ColumnDataType.INT:
            case ColumnDataType.LONG:
                type = DataType.INTEGER;
                break;
            case ColumnDataType.DECIMAL:
            case ColumnDataType.DOUBLE:
            case ColumnDataType.FLOAT:
                type = DataType.DOUBLE;
                break;
            case ColumnDataType.DATE:
            case ColumnDataType.TIME:
            case ColumnDataType.TIMESTAMP:
            case ColumnDataType.STRING:
            case ColumnDataType.MULTI_VALUE:
                type = DataType.STRING;
                break;
            default:
                throw new IllegalArgumentException("columnDataType=" + columnDataType);
        }
        return DataType.toString(type);
    }

    private static String generateTempTableName(String tableSchema, String tableName) {
        int randNum = 1000 + new Random(System.currentTimeMillis()).nextInt(1000);
        return tableSchema + "__" + tableName + "_" + System.currentTimeMillis() + randNum;
    }

}
