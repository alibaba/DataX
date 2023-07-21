package com.alibaba.datax.plugin.reader.otsreader.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;

import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConst;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;

/**
 * 主要对OTS PrimaryKey，OTSColumn的解析
 */
public class ReaderModelParser {
    
    private static long getLongValue(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Can not parse the value '"+ value +"' to Int.");
        }
    }
    
    private static double getDoubleValue(String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Can not parse the value '"+ value +"' to Double.");
        }
    }
    
    private static boolean getBoolValue(String value) {
        if (!(value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false"))) {
            throw new IllegalArgumentException("Can not parse the value '"+ value +"' to Bool.");
        }
        return Boolean.parseBoolean(value);
    }
    
    public static OTSColumn parseConstColumn(String type, String value) {
        if (type.equalsIgnoreCase(OTSConst.TYPE_STRING)) {
            return OTSColumn.fromConstStringColumn(value);
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_INTEGER)) {
            return OTSColumn.fromConstIntegerColumn(getLongValue(value));
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_DOUBLE)) {
            return OTSColumn.fromConstDoubleColumn(getDoubleValue(value));
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_BOOLEAN)) {
            return OTSColumn.fromConstBoolColumn(getBoolValue(value));
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_BINARY)) {
            return OTSColumn.fromConstBytesColumn(Base64.decodeBase64(value));
        } else {
            throw new IllegalArgumentException("Invalid 'column', Can not parse map to 'OTSColumn', input type:" + type + ", value:" + value + ".");
        }
    }
    
    public static OTSColumn parseOTSColumn(Map<String, Object> item) {
        if (item.containsKey(OTSConst.NAME)) {
            Object name = item.get(OTSConst.NAME);
            if (name instanceof String) {
                String nameStr = (String) name;
                return OTSColumn.fromNormalColumn(nameStr);
            } else {
                throw new IllegalArgumentException("Invalid 'column', Can not parse map to 'OTSColumn', the value is not a string.");
            }
        } else if (item.containsKey(OTSConst.TYPE) && item.containsKey(OTSConst.VALUE) && item.size() == 2) {
            Object type = item.get(OTSConst.TYPE);
            Object value = item.get(OTSConst.VALUE);
            if (type instanceof String && value instanceof String) {
                String typeStr = (String) type;
                String valueStr = (String) value;
                return parseConstColumn(typeStr, valueStr);
            } else {
                throw new IllegalArgumentException("Invalid 'column', Can not parse map to 'OTSColumn', the value is not a string.");
            }
        } else {
            throw new IllegalArgumentException(
                    "Invalid 'column', Can not parse map to 'OTSColumn', valid format: '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'.");
        }
    }
    
    private static void checkIsAllConstColumn(List<OTSColumn> columns) {
        for (OTSColumn c : columns) {
            if (c.getColumnType() == OTSColumn.OTSColumnType.NORMAL) {
                return ;
            }
        }
        throw new IllegalArgumentException("Invalid 'column', 'column' should include at least one or more Normal Column.");
    }

    public static List<OTSColumn> parseOTSColumnList(List<Object> input) {
        if (input.isEmpty()) {
            throw new IllegalArgumentException("Input count of 'column' is zero.");
        }
        
        List<OTSColumn> columns = new ArrayList<OTSColumn>(input.size());
        
        for (Object item:input) {
            if (item instanceof Map){ 
                @SuppressWarnings("unchecked")
                Map<String, Object> column = (Map<String, Object>) item;
                columns.add(parseOTSColumn(column));
            } else {
                throw new IllegalArgumentException("Invalid 'column', Can not parse Object to 'OTSColumn', item of list is not a map.");
            }
        }
        checkIsAllConstColumn(columns);
        return columns;
    }
    
    public static PrimaryKeyValue parsePrimaryKeyValue(String type, String value) {
        if (type.equalsIgnoreCase(OTSConst.TYPE_STRING)) {
            return PrimaryKeyValue.fromString(value);
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_INTEGER)) {
            return PrimaryKeyValue.fromLong(getLongValue(value));
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_INF_MIN)) {
            throw new IllegalArgumentException("Format error, the " + OTSConst.TYPE_INF_MIN + " only support {\"type\":\"" + OTSConst.TYPE_INF_MIN + "\"}.");
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_INF_MAX)) {
            throw new IllegalArgumentException("Format error, the " + OTSConst.TYPE_INF_MAX + " only support {\"type\":\"" + OTSConst.TYPE_INF_MAX + "\"}.");
        } else {
            throw new IllegalArgumentException("Not supprot parsing type: "+ type +" for PrimaryKeyValue.");
        }
    }
    
    public static PrimaryKeyValue parsePrimaryKeyValue(String type) {
        if (type.equalsIgnoreCase(OTSConst.TYPE_INF_MIN)) {
            return PrimaryKeyValue.INF_MIN;
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_INF_MAX)) {
            return PrimaryKeyValue.INF_MAX;
        } else {
            throw new IllegalArgumentException("Not supprot parsing type: "+ type +" for PrimaryKeyValue.");
        }
    }
    
    public static PrimaryKeyValue parsePrimaryKeyValue(Map<String, Object> item) {
        if (item.containsKey(OTSConst.TYPE) && item.containsKey(OTSConst.VALUE) && item.size() == 2) {
            Object type = item.get(OTSConst.TYPE);
            Object value = item.get(OTSConst.VALUE);
            if (type instanceof String && value instanceof String) {
                String typeStr = (String) type;
                String valueStr = (String) value;
                return parsePrimaryKeyValue(typeStr, valueStr);
            } else {
                throw new IllegalArgumentException("The 'type' and 'value‘ only support string.");
            }
        } else if (item.containsKey(OTSConst.TYPE) && item.size() == 1) {
            Object type = item.get(OTSConst.TYPE);
            if (type instanceof String) {
                String typeStr = (String) type;
                return parsePrimaryKeyValue(typeStr);
            } else {
                throw new IllegalArgumentException("The 'type' only support string.");
            }
        } else {
            throw new IllegalArgumentException("The map must consist of 'type' and 'value'.");
        }
    }

    public static List<PrimaryKeyValue> parsePrimaryKey(List<Object> input) {
        if (null == input) {
            return null;
        }
        List<PrimaryKeyValue> columns = new ArrayList<PrimaryKeyValue>(input.size());
        for (Object item:input) {
            if (item instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> column = (Map<String, Object>) item;
                columns.add(parsePrimaryKeyValue(column));
            } else {
                throw new IllegalArgumentException("Can not parse Object to 'PrimaryKeyValue', item of list is not a map.");
            }
        }
        return columns;
    }
}
