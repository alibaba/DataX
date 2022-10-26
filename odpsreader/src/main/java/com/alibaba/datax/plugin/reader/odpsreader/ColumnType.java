package com.alibaba.datax.plugin.reader.odpsreader;

public enum ColumnType {
    PARTITION, NORMAL, CONSTANT, UNKNOWN, ;

    public static ColumnType asColumnType(String columnTypeString) {
        if ("partition".equals(columnTypeString)) {
            return PARTITION;
        } else if ("normal".equals(columnTypeString)) {
            return NORMAL;
        } else if ("constant".equals(columnTypeString)) {
            return CONSTANT;
        } else {
            return UNKNOWN;
        }
    }
}
