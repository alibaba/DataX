package com.alibaba.datax.plugin.writer.otswriter.utils;

import com.alibaba.datax.plugin.writer.otswriter.model.*;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.TableMeta;

import java.util.*;

/**
 * 解析配置中参数
 * @author redchen
 *
 */
public class WriterModelParser {
    
    public static PrimaryKeyType parsePrimaryKeyType(String type) {
        if (type.equalsIgnoreCase(OTSConst.TYPE_STRING)) {
            return PrimaryKeyType.STRING;
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_INTEGER)) {
            return PrimaryKeyType.INTEGER;
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_BINARY)) {
            return PrimaryKeyType.BINARY;
        } else {
            throw new IllegalArgumentException(String.format(OTSErrorMessage.PK_TYPE_ERROR, type));
        }
    }
    
    private static Object columnGetObject(Map<String, Object> column, String key, String error) {
        Object value = column.get(key);
        
        if (value == null) {
            throw new IllegalArgumentException(error);
        }
       
        return value;
    }
    
    private static String checkString(Object value, String error) {
        if (!(value instanceof String)) {
            throw new IllegalArgumentException(error);
        }
        return (String)value;
    }
    
    private static void checkStringEmpty(String value, String error) {
        if (value.isEmpty()) {
            throw new IllegalArgumentException(error);
        }
    }
    
    public static PrimaryKeySchema parseOTSPKColumn(Map<String, Object> column) {
        String typeStr = checkString(
                columnGetObject(column, OTSConst.TYPE, String.format(OTSErrorMessage.PK_MAP_FILED_MISSING_ERROR, OTSConst.TYPE)),  
                String.format(OTSErrorMessage.PK_MAP_KEY_TYPE_ERROR, OTSConst.TYPE)
                );
        String nameStr = checkString(
                columnGetObject(column, OTSConst.NAME, String.format(OTSErrorMessage.PK_MAP_FILED_MISSING_ERROR, OTSConst.NAME)),  
                String.format(OTSErrorMessage.PK_MAP_KEY_TYPE_ERROR, OTSConst.NAME)
                );
        
        checkStringEmpty(typeStr, OTSErrorMessage.PK_COLUMN_TYPE_IS_EMPTY_ERROR);
        checkStringEmpty(nameStr, OTSErrorMessage.PK_COLUMN_NAME_IS_EMPTY_ERROR);
        
        if (column.size() == 2) {
            return new PrimaryKeySchema(nameStr, parsePrimaryKeyType(typeStr));
        } else {
            throw new IllegalArgumentException(OTSErrorMessage.PK_MAP_INCLUDE_NAME_TYPE_ERROR);
        }
    }
    
    public static List<PrimaryKeySchema> parseOTSPKColumnList(TableMeta meta, List<Object> values) {
        
        Map<String, PrimaryKeyType> pkMapping = meta.getPrimaryKeyMap();
        
        List<PrimaryKeySchema> pks = new ArrayList<PrimaryKeySchema>();
        for (Object obj : values) {
            /**
             * json 中primary key格式为：
             * "primaryKey":[
             *        "userid",
             *        "groupid"
             *]
             */
            if (obj instanceof String) {
                String name = (String) obj;
                PrimaryKeyType type = pkMapping.get(name);
                if (null == type) {
                    throw new IllegalArgumentException(String.format(OTSErrorMessage.PK_IS_NOT_EXIST_AT_OTS_ERROR, name)); 
                } else {
                    pks.add(new PrimaryKeySchema(name, type));
                }
            }
            /**
             * json 中primary key格式为：
             * "primaryKey" : [
             *        {"name":"pk1", "type":"string"},
             *        {"name":"pk2", "type":"int"}
             *],
             */
            else if (obj instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> column = (Map<String, Object>) obj;
                pks.add(parseOTSPKColumn(column));
            }
            else {
                throw new IllegalArgumentException(OTSErrorMessage.PK_ITEM_IS_ILLEAGAL_ERROR);
            }
        }
        return pks;
    }
    
    public static ColumnType parseColumnType(String type) {
        if (type.equalsIgnoreCase(OTSConst.TYPE_STRING)) {
            return ColumnType.STRING;
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_INTEGER)) {
            return ColumnType.INTEGER;
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_BOOLEAN)) {
            return ColumnType.BOOLEAN;
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_DOUBLE)) {
            return ColumnType.DOUBLE;
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_BINARY)) {
            return ColumnType.BINARY;
        } else {
            throw new IllegalArgumentException(String.format(OTSErrorMessage.ATTR_TYPE_ERROR, type));
        }
    }
    
    public static OTSAttrColumn parseOTSAttrColumn(Map<String, Object> column, OTSMode mode) {
        String typeStr = checkString(
                columnGetObject(column, OTSConst.TYPE, String.format(OTSErrorMessage.ATTR_MAP_FILED_MISSING_ERROR, OTSConst.TYPE)),  
                String.format(OTSErrorMessage.ATTR_MAP_KEY_TYPE_ERROR, OTSConst.TYPE)
                );
        String nameStr = checkString(
                columnGetObject(column, OTSConst.NAME, String.format(OTSErrorMessage.ATTR_MAP_FILED_MISSING_ERROR, OTSConst.NAME)),  
                String.format(OTSErrorMessage.ATTR_MAP_KEY_TYPE_ERROR, OTSConst.NAME)
                );
        
        checkStringEmpty(typeStr, OTSErrorMessage.ATTR_COLUMN_TYPE_IS_EMPTY_ERROR);
        checkStringEmpty(nameStr, OTSErrorMessage.ATTR_COLUMN_NAME_IS_EMPTY_ERROR);
        
        if (mode == OTSMode.MULTI_VERSION) {
            String srcNameStr = checkString(
                    columnGetObject(column, OTSConst.SRC_NAME, String.format(OTSErrorMessage.ATTR_MAP_FILED_MISSING_ERROR, OTSConst.SRC_NAME)),  
                    String.format(OTSErrorMessage.ATTR_MAP_KEY_TYPE_ERROR, OTSConst.SRC_NAME)
                    );
            checkStringEmpty(srcNameStr, OTSErrorMessage.ATTR_COLUMN_SRC_NAME_IS_EMPTY_ERROR);
            if (column.size() == 3) {
                return new OTSAttrColumn(srcNameStr, nameStr, parseColumnType(typeStr));
            } else {
                throw new IllegalArgumentException(OTSErrorMessage.ATTR_MAP_INCLUDE_SRCNAME_NAME_TYPE_ERROR);
            }
        } else {
            if (column.size() == 2) {
                return new OTSAttrColumn(nameStr, parseColumnType(typeStr));
            } else {
                throw new IllegalArgumentException(OTSErrorMessage.ATTR_MAP_INCLUDE_NAME_TYPE_ERROR);
            }
        }
    }

    public static List<OTSAttrColumn> parseOTSTimeseriesRowAttrList(List<Object> values) {
        List<OTSAttrColumn> attrs = new ArrayList<OTSAttrColumn>();
        // columns内部必须配置_m_name与_time字段，否则报错
        boolean getMeasurementField = false;
        boolean getTimeField = false;
        for (Object obj : values) {
            if (obj instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> column = (Map<String, Object>) obj;


                String nameStr = checkString(
                        columnGetObject(column, OTSConst.NAME, String.format(OTSErrorMessage.ATTR_MAP_FILED_MISSING_ERROR, OTSConst.NAME)),
                        String.format(OTSErrorMessage.ATTR_MAP_KEY_TYPE_ERROR, OTSConst.NAME)
                );
                boolean isTag = column.get(OTSConst.IS_TAG) != null && Boolean.parseBoolean((String) column.get(OTSConst.IS_TAG));
                String typeStr = "String";
                if (column.get(OTSConst.TYPE) != null){
                    typeStr = (String) column.get(OTSConst.TYPE);
                }

                checkStringEmpty(nameStr, OTSErrorMessage.ATTR_COLUMN_NAME_IS_EMPTY_ERROR);

                if (nameStr.equals(OTSConst.MEASUREMENT_NAME)){
                    getMeasurementField = true;
                } else if (nameStr.equals(OTSConst.TIME)) {
                    getTimeField = true;
                }

                attrs.add(new OTSAttrColumn(nameStr, parseColumnType(typeStr), isTag));
            } else {
                throw new IllegalArgumentException(OTSErrorMessage.ATTR_ITEM_IS_NOT_MAP_ERROR);
            }
        }
        if (!getMeasurementField){
            throw new IllegalArgumentException(OTSErrorMessage.NO_FOUND_M_NAME_FIELD_ERROR);
        } else if (!getTimeField) {
            throw new IllegalArgumentException(OTSErrorMessage.NO_FOUND_TIME_FIELD_ERROR);
        }
        return attrs;
    }
    
    private static void checkMultiAttrColumn(List<PrimaryKeySchema> pk, List<OTSAttrColumn> attrs, OTSMode mode) {
        // duplicate column name
        {
            Set<String> pool = new HashSet<String>();
            for (OTSAttrColumn col : attrs) {
                if (pool.contains(col.getName())) {
                    throw new IllegalArgumentException(String.format(OTSErrorMessage.MULTI_ATTR_COLUMN_ERROR, col.getName()));
                } else {
                    pool.add(col.getName());
                }
            }
            for (PrimaryKeySchema col : pk) {
                if (pool.contains(col.getName())) {
                    throw new IllegalArgumentException(String.format(OTSErrorMessage.MULTI_PK_ATTR_COLUMN_ERROR, col.getName()));
                } else {
                    pool.add(col.getName());
                }
            }
        }
        // duplicate src column name
        if (mode == OTSMode.MULTI_VERSION) {
            Set<String> pool = new HashSet<String>();
            for (OTSAttrColumn col : attrs) {
                if (pool.contains(col.getSrcName())) {
                    throw new IllegalArgumentException(String.format(OTSErrorMessage.MULTI_ATTR_SRC_COLUMN_ERROR, col.getSrcName()));
                } else {
                    pool.add(col.getSrcName());
                }
            }
        }
    }
    
    public static List<OTSAttrColumn> parseOTSAttrColumnList(List<PrimaryKeySchema> pk, List<Object> values, OTSMode mode) {
        List<OTSAttrColumn> attrs = new ArrayList<OTSAttrColumn>();
        for (Object obj : values) {
            if (obj instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> column = (Map<String, Object>) obj;
                attrs.add(parseOTSAttrColumn(column, mode));
            } else {
                throw new IllegalArgumentException(OTSErrorMessage.ATTR_ITEM_IS_NOT_MAP_ERROR);
            }
        }
        checkMultiAttrColumn(pk, attrs, mode);
        return attrs;
    }
    
    public static OTSOpType parseOTSOpType(String value, OTSMode mode) {
        OTSOpType type = null;
        if (value.equalsIgnoreCase(OTSConst.OTS_OP_TYPE_PUT)) {
            type = OTSOpType.PUT_ROW;
        } else if (value.equalsIgnoreCase(OTSConst.OTS_OP_TYPE_UPDATE)) {
            type = OTSOpType.UPDATE_ROW;
        }else if (value.equalsIgnoreCase(OTSConst.OTS_OP_TYPE_DELETE)) {
            type = OTSOpType.DELETE_ROW;
        }else {
            throw new IllegalArgumentException(String.format(OTSErrorMessage.OPERATION_PARSE_ERROR, value));
        }
        
        if (mode == OTSMode.MULTI_VERSION && type == OTSOpType.PUT_ROW) {
            throw new IllegalArgumentException(String.format(OTSErrorMessage.MUTLI_MODE_OPERATION_PARSE_ERROR, value));
        }
        return type;
    }
    
    public static OTSMode parseOTSMode(String value) {
        if (value.equalsIgnoreCase(OTSConst.OTS_MODE_NORMAL)) {
            return OTSMode.NORMAL;
        } else if (value.equalsIgnoreCase(OTSConst.OTS_MODE_MULTI_VERSION)) {
            return OTSMode.MULTI_VERSION;
        } else {
            throw new IllegalArgumentException(String.format(OTSErrorMessage.MODE_PARSE_ERROR, value));
        }
    }

}
