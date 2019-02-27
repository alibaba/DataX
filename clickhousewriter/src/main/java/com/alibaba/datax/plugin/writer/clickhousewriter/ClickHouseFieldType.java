package com.alibaba.datax.plugin.writer.clickhousewriter;

public enum ClickHouseFieldType {
    UINT8,
    UINT16,
    UINT32,
    UINT64,
    INT8,
    INT16,
    INT32,
    INT64,
    FLOAT32,
    FLOAT64,
    DECIMAL,
    DATE,
    DATETIME,
    ARRAY;

    public static ClickHouseFieldType getCHFieldType(String type) {
        if (type == null) {
            return null;
        }
        for (ClickHouseFieldType f : ClickHouseFieldType.values()) {
            if (f.name().compareTo(type.toUpperCase()) == 0) {
                return f;
            }
        }
        return null;
    }
}
