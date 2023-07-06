package com.alibaba.datax.plugin.writer.neo4jwriter.element;

import java.util.Arrays;

/**
 * @see org.neo4j.driver.Values
 * @author fuyouj
 */
public enum PropertyType {
    NULL,
    BOOLEAN,
    STRING,
    LONG,
    SHORT,
    INTEGER,
    DOUBLE,
    FLOAT,
    LOCAL_DATE,
    LOCAL_TIME,
    LOCAL_DATE_TIME,
    LIST,
    MAP,
    CHAR_ARRAY,
    BYTE_ARRAY,
    BOOLEAN_ARRAY,
    STRING_ARRAY,
    LONG_ARRAY,
    INT_ARRAY,
    SHORT_ARRAY,
    DOUBLE_ARRAY,
    FLOAT_ARRAY,
    Object_ARRAY;

    public static PropertyType fromStrIgnoreCase(String typeStr) {
        return Arrays.stream(PropertyType.values())
                .filter(e -> e.name().equalsIgnoreCase(typeStr))
                .findFirst()
                .orElse(PropertyType.STRING);
    }
}
