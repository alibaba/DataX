/**
 * 
 */
package com.alibaba.datax.plugin.writer.gdbwriter.mapping;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.alibaba.datax.common.element.Column;

import lombok.extern.slf4j.Slf4j;

/**
 * @author jerrywang
 *
 */
@Slf4j
public enum ValueType {
    /**
     * property value type
     */
    INT(Integer.class, "int", Column::asLong, Integer::valueOf),
    INTEGER(Integer.class, "integer", Column::asLong, Integer::valueOf),
    LONG(Long.class, "long", Column::asLong, Long::valueOf),
    DOUBLE(Double.class, "double", Column::asDouble, Double::valueOf),
    FLOAT(Float.class, "float", Column::asDouble, Float::valueOf),
    BOOLEAN(Boolean.class, "boolean", Column::asBoolean, Boolean::valueOf),
    STRING(String.class, "string", Column::asString, String::valueOf);

    private Class<?> type = null;
    private String shortName = null;
    private Function<Column, Object> columnFunc = null;
    private Function<String, Object> fromStrFunc = null;

    private ValueType(final Class<?> type, final String name, final Function<Column, Object> columnFunc,
                      final Function<String, Object> fromStrFunc) {
        this.type = type;
        this.shortName = name;
        this.columnFunc = columnFunc;
        this.fromStrFunc = fromStrFunc;

        ValueTypeHolder.shortName2type.put(name, this);
    }

    public static ValueType fromShortName(final String name) {
        return ValueTypeHolder.shortName2type.get(name);
    }

    public Class<?> type() {
        return this.type;
    }

    public String shortName() {
        return this.shortName;
    }

    public Object applyColumn(final Column column) {
        try {
            if (column == null) {
                return null;
            }
            return this.columnFunc.apply(column);
        } catch (final Exception e) {
            log.error("applyColumn error {}, column {}", e.toString(), column);
            throw e;
        }
    }

    public Object fromStrFunc(final String str) {
        return this.fromStrFunc.apply(str);
    }

    private static class ValueTypeHolder {
        private static Map<String, ValueType> shortName2type = new HashMap<>();
    }
}
