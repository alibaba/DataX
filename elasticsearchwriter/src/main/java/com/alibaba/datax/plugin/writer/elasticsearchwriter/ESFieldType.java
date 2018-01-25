package com.alibaba.datax.plugin.writer.elasticsearchwriter;

/**
 * Created by xiongfeng.bxf on 17/3/1.
 */
public enum ESFieldType {
    ID,
    STRING,
    TEXT,
    KEYWORD,
    LONG,
    INTEGER,
    SHORT,
    BYTE,
    DOUBLE,
    FLOAT,
    DATE,
    BOOLEAN,
    BINARY,
    INTEGER_RANGE,
    FLOAT_RANGE,
    LONG_RANGE,
    DOUBLE_RANGE,
    DATE_RANGE,
    GEO_POINT,
    GEO_SHAPE,

    IP,
    COMPLETION,
    TOKEN_COUNT,

    ARRAY,
    OBJECT,
    NESTED;

    public static ESFieldType getESFieldType(String type) {
        if (type == null) {
            return null;
        }
        for (ESFieldType f : ESFieldType.values()) {
            if (f.name().compareTo(type.toUpperCase()) == 0) {
                return f;
            }
        }
        return null;
    }
}
