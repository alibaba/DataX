package com.alibaba.datax.plugin.reader.tablestorereader.utils;

import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStoreColumn;
import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStoreConst;
import org.apache.commons.codec.binary.Base64;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 主要对OTS PrimaryKey，OTSColumn的解析
 */
public class ReaderModelParser {

    private static long getLongValue(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Can not parse the value '" + value + "' to Int.");
        }
    }

    private static double getDoubleValue(String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Can not parse the value '" + value + "' to Double.");
        }
    }

    private static boolean getBoolValue(String value) {
        if (!(value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false"))) {
            throw new IllegalArgumentException("Can not parse the value '" + value + "' to Bool.");
        }
        return Boolean.parseBoolean(value);
    }

    public static TableStoreColumn parseConstColumn(String type, String value) {
        if (type.equalsIgnoreCase(TableStoreConst.TYPE_STRING)) {
            return TableStoreColumn.fromConstStringColumn(value);
        } else if (type.equalsIgnoreCase(TableStoreConst.TYPE_INTEGER)) {
            return TableStoreColumn.fromConstIntegerColumn(getLongValue(value));
        } else if (type.equalsIgnoreCase(TableStoreConst.TYPE_DOUBLE)) {
            return TableStoreColumn.fromConstDoubleColumn(getDoubleValue(value));
        } else if (type.equalsIgnoreCase(TableStoreConst.TYPE_BOOLEAN)) {
            return TableStoreColumn.fromConstBoolColumn(getBoolValue(value));
        } else if (type.equalsIgnoreCase(TableStoreConst.TYPE_BINARY)) {
            return TableStoreColumn.fromConstBytesColumn(Base64.decodeBase64(value));
        } else {
            throw new IllegalArgumentException("Invalid 'column', Can not parse map to 'TableStoreColumn', input type:" + type + ", value:" + value + ".");
        }
    }

    public static TableStoreColumn parseOTSColumn(Map<String, Object> item) {
        if (item.containsKey(TableStoreConst.NAME) && item.size() == 1) {
            Object name = item.get(TableStoreConst.NAME);
            if (name instanceof String) {
                String nameStr = (String) name;
                return TableStoreColumn.fromNormalColumn(nameStr);
            } else {
                throw new IllegalArgumentException("Invalid 'column', Can not parse map to 'TableStoreColumn', the value is not a string.");
            }
        } else if (item.containsKey(TableStoreConst.TYPE) && item.containsKey(TableStoreConst.VALUE) && item.size() == 2) {
            Object type = item.get(TableStoreConst.TYPE);
            Object value = item.get(TableStoreConst.VALUE);
            if (type instanceof String && value instanceof String) {
                String typeStr = (String) type;
                String valueStr = (String) value;
                return parseConstColumn(typeStr, valueStr);
            } else {
                throw new IllegalArgumentException("Invalid 'column', Can not parse map to 'TableStoreColumn', the value is not a string.");
            }
        } else {
            throw new IllegalArgumentException(
                    "Invalid 'column', Can not parse map to 'TableStoreColumn', valid format: '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'.");
        }
    }

    private static void checkIsAllConstColumn(List<TableStoreColumn> columns) {
        for (TableStoreColumn c : columns) {
            if (c.getColumnType() == TableStoreColumn.OTSColumnType.NORMAL) {
                return;
            }
        }
        throw new IllegalArgumentException("Invalid 'column', 'column' should include at least one or more Normal Column.");
    }

    public static List<TableStoreColumn> parseOTSColumnList(List<Object> input) {
        if (input.isEmpty()) {
            throw new IllegalArgumentException("Input count of 'column' is zero.");
        }

        List<TableStoreColumn> columns = new ArrayList<TableStoreColumn>(input.size());

        for (Object item : input) {
            if (item instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> column = (Map<String, Object>) item;
                columns.add(parseOTSColumn(column));
            } else {
                throw new IllegalArgumentException("Invalid 'column', Can not parse Object to 'TableStoreColumn', item of list is not a map.");
            }
        }
        checkIsAllConstColumn(columns);
        return columns;
    }

//
//    public static PrimaryKeyValue parsePrimaryKeyValue(String type, String value) {
//        if (type.equalsIgnoreCase(TableStoreConst.TYPE_STRING)) {
//            return PrimaryKeyValue.fromString(value);
//        } else if (type.equalsIgnoreCase(TableStoreConst.TYPE_INTEGER)) {
//            return PrimaryKeyValue.fromLong(getLongValue(value));
//        } else if (type.equalsIgnoreCase(TableStoreConst.TYPE_INF_MIN)) {
//            throw new IllegalArgumentException("Format error, the " + TableStoreConst.TYPE_INF_MIN + " only support {\"type\":\"" + TableStoreConst.TYPE_INF_MIN + "\"}.");
//        } else if (type.equalsIgnoreCase(TableStoreConst.TYPE_INF_MAX)) {
//            throw new IllegalArgumentException("Format error, the " + TableStoreConst.TYPE_INF_MAX + " only support {\"type\":\"" + TableStoreConst.TYPE_INF_MAX + "\"}.");
//        } else {
//            throw new IllegalArgumentException("Not supprot parsing type: " + type + " for PrimaryKeyValue.");
//        }
//    }

//    public static PrimaryKeyValue parsePrimaryKeyValue(String type) {
//        if (type.equalsIgnoreCase(TableStoreConst.TYPE_INF_MIN)) {
//            return PrimaryKeyValue.INF_MIN;
//        } else if (type.equalsIgnoreCase(TableStoreConst.TYPE_INF_MAX)) {
//            return PrimaryKeyValue.INF_MAX;
//        } else {
//            throw new IllegalArgumentException("Not supprot parsing type: " + type + " for PrimaryKeyValue.");
//        }
//    }

//    public static PrimaryKeyValue parsePrimaryKeyValue(Map<String, Object> item) {
//        if (item.containsKey(TableStoreConst.TYPE) && item.containsKey(TableStoreConst.VALUE) && item.size() == 2) {
//            Object type = item.get(TableStoreConst.TYPE);
//            Object value = item.get(TableStoreConst.VALUE);
//            if (type instanceof String && value instanceof String) {
//                String typeStr = (String) type;
//                String valueStr = (String) value;
//                return parsePrimaryKeyValue(typeStr, valueStr);
//            } else {
//                throw new IllegalArgumentException("The 'type' and 'value‘ only support string.");
//            }
//        } else if (item.containsKey(TableStoreConst.TYPE) && item.size() == 1) {
//            Object type = item.get(TableStoreConst.TYPE);
//            if (type instanceof String) {
//                String typeStr = (String) type;
//                return parsePrimaryKeyValue(typeStr);
//            } else {
//                throw new IllegalArgumentException("The 'type' only support string.");
//            }
//        } else {
//            throw new IllegalArgumentException("The map must consist of 'type' and 'value'.");
//        }
//    }

//    public static List<PrimaryKeyValue> parsePrimaryKey(List<Object> input) {
//        if (null == input) {
//            return null;
//        }
//        List<PrimaryKeyValue> columns = new ArrayList<PrimaryKeyValue>(input.size());
//        for (Object item : input) {
//            if (item instanceof Map) {
//                @SuppressWarnings("unchecked")
//                Map<String, Object> column = (Map<String, Object>) item;
//                columns.add(parsePrimaryKeyValue(column));
//            } else {
//                throw new IllegalArgumentException("Can not parse Object to 'PrimaryKeyValue', item of list is not a map.");
//            }
//        }
//        return columns;
//    }
}
