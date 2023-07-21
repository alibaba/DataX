package com.alibaba.datax.plugin.reader.otsreader.utils;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSCriticalException;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import org.apache.commons.codec.binary.Base64;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ParamParser {
    
    // ------------------------------------------------------------------------
    // Range解析相关的逻辑
    // ------------------------------------------------------------------------
    
    private static PrimaryKeyValue parsePrimaryKeyValue(String type) {
        return parsePrimaryKeyValue(type, null);
    }
    
    private static PrimaryKeyValue  parsePrimaryKeyValue(String type, String value) {
        if (type.equalsIgnoreCase(Constant.ValueType.INF_MIN)) {
            return PrimaryKeyValue.INF_MIN;
        } else if (type.equalsIgnoreCase(Constant.ValueType.INF_MAX)) {
            return PrimaryKeyValue.INF_MAX;
        } else {
            if (value != null) {
                if (type.equalsIgnoreCase(Constant.ValueType.STRING)) {
                    return PrimaryKeyValue.fromString(value);
                } else if (type.equalsIgnoreCase(Constant.ValueType.INTEGER)) {
                    return PrimaryKeyValue.fromLong(Long.valueOf(value));
                } else if (type.equalsIgnoreCase(Constant.ValueType.BINARY)) {
                    return PrimaryKeyValue.fromBinary(Base64.decodeBase64(value));
                } else {
                    throw new IllegalArgumentException("the column type only support :['INF_MIN', 'INF_MAX', 'string', 'int', 'binary']");
                }
            } else {
                throw new IllegalArgumentException("the column is missing the field 'value', input 'type':" + type);
            }
        }
    }
    
    private static PrimaryKeyColumn parsePrimaryKeyColumn(Map<String, Object> item) {
        Object typeObj = item.get(Constant.ConfigKey.PrimaryKeyColumn.TYPE);
        Object valueObj = item.get(Constant.ConfigKey.PrimaryKeyColumn.VALUE);
        
        if (typeObj != null && valueObj != null) {
            if (typeObj instanceof String && valueObj instanceof String) {
                return new PrimaryKeyColumn(
                        Constant.ConfigDefaultValue.DEFAULT_NAME, 
                        parsePrimaryKeyValue((String)typeObj, (String)valueObj)
                        );
            } else {
                throw new IllegalArgumentException(
                        "the column's 'type' and 'value' must be string value, "
                                + "but type of 'type' is :" + typeObj.getClass() + 
                                ", type of 'value' is :" + valueObj.getClass()
                        );
            }
        } else if (typeObj != null) {
            if (typeObj instanceof String) {
                return new PrimaryKeyColumn(
                        Constant.ConfigDefaultValue.DEFAULT_NAME, 
                        parsePrimaryKeyValue((String)typeObj)
                        );
            } else {
                throw new IllegalArgumentException(
                        "the column's 'type' must be string value, "
                                + "but type of 'type' is :" + typeObj.getClass()
                        );
            }
        } else {
            throw new IllegalArgumentException("the column must include 'type' and 'value'.");
        }
    }
    
    @SuppressWarnings("unchecked")
    public static List<PrimaryKeyColumn> parsePrimaryKeyColumnArray(Object arrayObj) throws OTSCriticalException {
        try {
            List<PrimaryKeyColumn> columns = new  ArrayList<PrimaryKeyColumn>();
            if (arrayObj instanceof List) {
                List<Object> array = (List<Object>) arrayObj;
                for (Object o : array) {
                    if (o instanceof Map) {
                        Map<String, Object> column = (Map<String, Object>) o;
                        columns.add(parsePrimaryKeyColumn(column));
                    } else {
                        throw new IllegalArgumentException("input primary key column must be map object, but input type:" + o.getClass());
                    }
                }
            } else {
                throw new IllegalArgumentException("input 'begin','end','split' must be list object, but input type:" + arrayObj.getClass());
            }
            return columns;
        } catch (RuntimeException e) {
            // 因为基础模块本身可能抛出一些错误，为了方便定位具体的出错位置，在此把Range加入到Error Message中
            throw new OTSCriticalException("Parse 'range' fail, " + e.getMessage(), e);
        }
    }
    
    // ------------------------------------------------------------------------
    // Column解析相关的逻辑
    // ------------------------------------------------------------------------
    
    private static OTSColumn parseOTSColumn(Object obj) {
        if (obj instanceof String) {
            return OTSColumn.fromNormalColumn((String)obj);
        } else {
            throw new IllegalArgumentException("the 'name' must be string, but input:" + obj.getClass());
        }
    }
    
    private static OTSColumn parseOTSColumn(Object typeObj, Object valueObj) {
        if (typeObj instanceof String && valueObj instanceof String) {
            String type = (String)typeObj;
            String value = (String)valueObj;
            
            if (type.equalsIgnoreCase(Constant.ValueType.STRING)) {
                return OTSColumn.fromConstStringColumn(value);
            } else if (type.equalsIgnoreCase(Constant.ValueType.INTEGER)) {
                return OTSColumn.fromConstIntegerColumn(Long.valueOf(value));
            } else if (type.equalsIgnoreCase(Constant.ValueType.DOUBLE)) {
                return OTSColumn.fromConstDoubleColumn(Double.valueOf(value));
            } else if (type.equalsIgnoreCase(Constant.ValueType.BOOLEAN)) {
                return OTSColumn.fromConstBoolColumn(Boolean.valueOf(value));
            } else if (type.equalsIgnoreCase(Constant.ValueType.BINARY)) {
                return OTSColumn.fromConstBytesColumn(Base64.decodeBase64(value));
            } else {
                throw new IllegalArgumentException("the const column type only support :['string', 'int', 'double', 'bool', 'binary']");
            }
        } else {
            throw new IllegalArgumentException("the 'type' and 'value' must be string, but 'type''s type:" + typeObj.getClass() + " 'value''s type:" + valueObj.getClass());
        }
    }
    
    private static OTSColumn parseOTSColumn(Map<String, Object> column) {
        Object typeObj = column.get(Constant.ConfigKey.Column.TYPE);
        Object valueObj = column.get(Constant.ConfigKey.Column.VALUE);
        Object nameObj = column.get(Constant.ConfigKey.Column.NAME);
        
        if (nameObj != null) {
            return parseOTSColumn(nameObj);
        } else if (typeObj != null && valueObj != null) {
            return parseOTSColumn(typeObj, valueObj);
        } else {
            throw new IllegalArgumentException("the item of column format support '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'.");
        }
    }
    
    @SuppressWarnings("unchecked")
    public static List<OTSColumn> parseOTSColumnArray(List<Object> value) throws OTSCriticalException {
        try {
            List<OTSColumn> result = new ArrayList<OTSColumn>();
            for (Object item:value) {
                if (item instanceof Map){ 
                    Map<String, Object> column = (Map<String, Object>) item;
                    result.add(ParamParser.parseOTSColumn(column));
                } else {
                    throw new IllegalArgumentException("the item of column must be map object, but input: " + item.getClass());
                }
            }
            return result;
        } catch (RuntimeException e) {
            // 因为基础模块本身可能抛出一些错误，为了方便定位具体的出错位置，在此把Column加入到Error Message中
            throw new OTSCriticalException("Parse 'column' fail. " + e.getMessage(), e);
        }
    }

    private static ColumnType parseTimeseriesColumnType(Map<String, Object> column) {
        Object typeObj = column.getOrDefault(Constant.ConfigKey.Column.TYPE, "");
        if (typeObj instanceof String) {
            String type = (String)typeObj;

            if (type.equalsIgnoreCase(Constant.ValueType.STRING)) {
                return ColumnType.STRING;
            } else if (type.equalsIgnoreCase(Constant.ValueType.INTEGER)) {
                return ColumnType.INTEGER;
            } else if (type.equalsIgnoreCase(Constant.ValueType.DOUBLE)) {
                return ColumnType.DOUBLE;
            } else if (type.equalsIgnoreCase(Constant.ValueType.BOOLEAN)) {
                return ColumnType.BOOLEAN;
            } else if (type.equalsIgnoreCase(Constant.ValueType.BINARY)) {
                return ColumnType.BINARY;
            } else if (type.length() == 0){
                return ColumnType.STRING;
            }else {
                throw new IllegalArgumentException("the timeseries column type only support :['string', 'int', 'double', 'bool', 'binary']");
            }
        } else {
            throw new IllegalArgumentException("the 'type' must be string, but 'type''s type:" + typeObj.getClass());
        }
    }

    public static List<ColumnType> parseColumnTypeArray(List<Object> value) throws OTSCriticalException {
        try {
            List<ColumnType> result = new ArrayList<ColumnType>();
            for (Object item:value) {
                if (item instanceof Map){
                    Map<String, Object> column = (Map<String, Object>) item;
                    result.add(ParamParser.parseTimeseriesColumnType(column));
                } else {
                    throw new IllegalArgumentException("the item of column must be map object, but input: " + item.getClass());
                }
            }
            return result;
        } catch (RuntimeException e) {
            throw new OTSCriticalException("Parse 'timeseries column type' fail. " + e.getMessage(), e);
        }
    }

    private static Boolean parseTimeseriesColumnIsTag(Map<String, Object> column) {
        Object isTagParameter = column.getOrDefault(Constant.ConfigKey.Column.IS_TAG, "");
        if (isTagParameter instanceof String) {
            String isTag = (String)isTagParameter;
            return Boolean.valueOf(isTag);
        } else {
            throw new IllegalArgumentException("the 'isTag' must be string, but 'isTag''s type:" + isTagParameter.getClass());
        }
    }

    public static List<Boolean> parseColumnIsTagArray(List<Object> value) throws OTSCriticalException {
        try {
            List<Boolean> result = new ArrayList<Boolean>();
            for (Object item:value) {
                if (item instanceof Map){
                    Map<String, Object> column = (Map<String, Object>) item;
                    result.add(ParamParser.parseTimeseriesColumnIsTag(column));
                } else {
                    throw new IllegalArgumentException("the item of column must be map object, but input: " + item.getClass());
                }
            }
            return result;
        } catch (RuntimeException e) {
            throw new OTSCriticalException("Parse 'timeseries column isTag' fail. " + e.getMessage(), e);
        }
    }
    
    // ------------------------------------------------------------------------
    // TimeRange解析相关的逻辑
    // ------------------------------------------------------------------------

    public static long parseTimeRangeItem(Object obj, String key) {
        if (obj instanceof Integer) {
            return (Integer)obj;
        } else if (obj instanceof Long) {
            return (Long)obj;
        } else {
            throw new IllegalArgumentException("the '"+ key +"' must be int, but input:" + obj.getClass());
        }
    }
}
