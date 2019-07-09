package com.alibaba.datax.plugin.reader.tablestorereader.model;

import com.alibaba.datax.common.element.*;
import com.alicloud.openservices.tablestore.model.ColumnType;

public class TableStoreColumn {
    private String name;
    private Column value;
    private OTSColumnType columnType;
    private ColumnType valueType;
    
    public static enum OTSColumnType {
        NORMAL, // 普通列
        CONST   // 常量列
    }
    
    private TableStoreColumn(String name) {
        this.name = name;
        this.columnType = OTSColumnType.NORMAL;
    }
    
    private TableStoreColumn(Column value, ColumnType type) {
        this.value = value;
        this.columnType = OTSColumnType.CONST;
        this.valueType = type;
    }
    
    public static TableStoreColumn fromNormalColumn(String name) {
        if (name.isEmpty()) {
            throw new IllegalArgumentException("The column name is empty.");
        }
        
        return new TableStoreColumn(name);
    } 
    
    public static TableStoreColumn fromConstStringColumn(String value) {
        return new TableStoreColumn(new StringColumn(value), ColumnType.STRING);
    } 
    
    public static TableStoreColumn fromConstIntegerColumn(long value) {
        return new TableStoreColumn(new LongColumn(value), ColumnType.INTEGER);
    } 
    
    public static TableStoreColumn fromConstDoubleColumn(double value) {
        return new TableStoreColumn(new DoubleColumn(value), ColumnType.DOUBLE);
    } 
    
    public static TableStoreColumn fromConstBoolColumn(boolean value) {
        return new TableStoreColumn(new BoolColumn(value), ColumnType.BOOLEAN);
    } 
    
    public static TableStoreColumn fromConstBytesColumn(byte[] value) {
        return new TableStoreColumn(new BytesColumn(value), ColumnType.BINARY);
    } 
    
    public Column getValue() {
        return value;
    }
    
    public OTSColumnType getColumnType() {
        return columnType;
    }
    
    public ColumnType getValueType() {
        return valueType;
    }

    public String getName() {
        return name;
    }
}
