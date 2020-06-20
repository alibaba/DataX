/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */
package com.alibaba.datax.plugin.reader.gdbreader.mapping;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.StringColumn;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * @author : Liu Jianping
 * @date : 2019/9/6
 */

public enum ValueType {
    /**
     * transfer gdb element object value to DataX Column data
     * <p>
     * int, long -> LongColumn
     * float, double -> DoubleColumn
     * bool -> BooleanColumn
     * string -> StringColumn
     */
    INT(Integer.class, "int", ValueTypeHolder::longColumnMapper),
    INTEGER(Integer.class, "integer", ValueTypeHolder::longColumnMapper),
    LONG(Long.class, "long", ValueTypeHolder::longColumnMapper),
    DOUBLE(Double.class, "double", ValueTypeHolder::doubleColumnMapper),
    FLOAT(Float.class, "float", ValueTypeHolder::doubleColumnMapper),
    BOOLEAN(Boolean.class, "boolean", ValueTypeHolder::boolColumnMapper),
    STRING(String.class, "string", ValueTypeHolder::stringColumnMapper),
    ;

    private Class<?> type = null;
    private String shortName = null;
    private Function<Object, Column> columnFunc = null;

    ValueType(Class<?> type, String name, Function<Object, Column> columnFunc) {
        this.type = type;
        this.shortName = name;
        this.columnFunc = columnFunc;

        ValueTypeHolder.shortName2type.put(shortName, this);
    }

    public static ValueType fromShortName(String name) {
        return ValueTypeHolder.shortName2type.get(name);
    }

    public Column applyObject(Object value) {
        if (value == null) {
            return null;
        }
        return columnFunc.apply(value);
    }

    private static class ValueTypeHolder {
        private static Map<String, ValueType> shortName2type = new HashMap<>();

        private static LongColumn longColumnMapper(Object o) {
            long v;
            if (o instanceof Integer) {
                v = (int) o;
            } else if (o instanceof Long) {
                v = (long) o;
            } else if (o instanceof String) {
                v = Long.valueOf((String) o);
            } else {
                throw new RuntimeException("Failed to cast " + o.getClass() + " to Long");
            }

            return new LongColumn(v);
        }

        private static DoubleColumn doubleColumnMapper(Object o) {
            double v;
            if (o instanceof Integer) {
                v = (double) (int) o;
            } else if (o instanceof Long) {
                v = (double) (long) o;
            } else if (o instanceof Float) {
                v = (double) (float) o;
            } else if (o instanceof Double) {
                v = (double) o;
            } else if (o instanceof String) {
                v = Double.valueOf((String) o);
            } else {
                throw new RuntimeException("Failed to cast " + o.getClass() + " to Double");
            }

            return new DoubleColumn(v);
        }

        private static BoolColumn boolColumnMapper(Object o) {
            boolean v;
            if (o instanceof Integer) {
                v = ((int) o != 0);
            } else if (o instanceof Long) {
                v = ((long) o != 0);
            } else if (o instanceof Boolean) {
                v = (boolean) o;
            } else if (o instanceof String) {
                v = Boolean.valueOf((String) o);
            } else {
                throw new RuntimeException("Failed to cast " + o.getClass() + " to Boolean");
            }

            return new BoolColumn(v);
        }

        private static StringColumn stringColumnMapper(Object o) {
            if (o instanceof String) {
                return new StringColumn((String) o);
            } else {
                return new StringColumn(String.valueOf(o));
            }
        }
    }
}
