package com.alibaba.datax.plugin.writer.adswriter.odps;

/**
 * ODPS 数据类型.
 * <p>
 * 当前定义了如下类型：
 * <ul>
 * <li>INTEGER
 * <li>DOUBLE
 * <li>BOOLEAN
 * <li>STRING
 * <li>DATETIME
 * </ul>
 * </p>
 *
 * @since 0.0.1
 */
public class DataType {

    public final static byte INTEGER = 0;
    public final static byte DOUBLE = 1;
    public final static byte BOOLEAN = 2;
    public final static byte STRING = 3;
    public final static byte DATETIME = 4;

    public static String toString(int type) {
        switch (type) {
            case INTEGER:
                return "bigint";
            case DOUBLE:
                return "double";
            case BOOLEAN:
                return "boolean";
            case STRING:
                return "string";
            case DATETIME:
                return "datetime";
            default:
                throw new IllegalArgumentException("type=" + type);
        }
    }

    /**
     * 字符串的数据类型转换为byte常量定义的数据类型.
     * <p>
     * 转换规则：
     * <ul>
     * <li>tinyint, int, bigint, long - {@link #INTEGER}
     * <li>double, float - {@link #DOUBLE}
     * <li>string - {@link #STRING}
     * <li>boolean, bool - {@link #BOOLEAN}
     * <li>datetime - {@link #DATETIME}
     * </ul>
     * </p>
     *
     * @param type 字符串的数据类型
     * @return byte常量定义的数据类型
     * @throws IllegalArgumentException
     */
    public static byte convertToDataType(String type) throws IllegalArgumentException {
        type = type.toLowerCase().trim();
        if ("string".equals(type)) {
            return STRING;
        } else if ("bigint".equals(type) || "int".equals(type) || "tinyint".equals(type) || "long".equals(type)) {
            return INTEGER;
        } else if ("boolean".equals(type) || "bool".equals(type)) {
            return BOOLEAN;
        } else if ("double".equals(type) || "float".equals(type)) {
            return DOUBLE;
        } else if ("datetime".equals(type)) {
            return DATETIME;
        } else {
            throw new IllegalArgumentException("unknown type: " + type);
        }
    }

}
