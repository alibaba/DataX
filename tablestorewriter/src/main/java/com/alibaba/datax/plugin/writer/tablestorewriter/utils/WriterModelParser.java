package com.alibaba.datax.plugin.writer.tablestorewriter.utils;

import com.alibaba.datax.plugin.writer.tablestorewriter.model.*;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;

import java.util.*;

/**
 * 解析配置中参数
 *
 * @author redchen
 */
public class WriterModelParser {

    public static PrimaryKeyType parsePrimaryKeyType(String type) {
        if (type.equalsIgnoreCase(TableStoreConst.TYPE_STRING)) {
            return PrimaryKeyType.STRING;
        } else if (type.equalsIgnoreCase(TableStoreConst.TYPE_INTEGER)) {
            return PrimaryKeyType.INTEGER;
        } else {
            throw new IllegalArgumentException(String.format(TableStoreErrorMessage.PK_TYPE_ERROR, type));
        }
    }

    public static TableStorePKColumn parseTableStorePKColumn(Map<String, Object> column) {
        if (column.containsKey(TableStoreConst.NAME) && column.containsKey(TableStoreConst.TYPE) && column.size() == 2) {
            Object type = column.get(TableStoreConst.TYPE);
            Object name = column.get(TableStoreConst.NAME);
            if (type instanceof String && name instanceof String) {
                String typeStr = (String) type;
                String nameStr = (String) name;
                if (nameStr.isEmpty()) {
                    throw new IllegalArgumentException(TableStoreErrorMessage.PK_COLUMN_NAME_IS_EMPTY_ERROR);
                }
                return new TableStorePKColumn(nameStr, parsePrimaryKeyType(typeStr));
            } else {
                throw new IllegalArgumentException(TableStoreErrorMessage.PK_MAP_NAME_TYPE_ERROR);
            }
        } else {
            throw new IllegalArgumentException(TableStoreErrorMessage.PK_MAP_INCLUDE_NAME_TYPE_ERROR);
        }
    }

    public static List<TableStorePKColumn> parseTableStorePKColumnList(List<Object> values) {
        List<TableStorePKColumn> pks = new ArrayList<TableStorePKColumn>();
        for (Object obj : values) {
            if (obj instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> column = (Map<String, Object>) obj;
                pks.add(parseTableStorePKColumn(column));
            } else {
                throw new IllegalArgumentException(TableStoreErrorMessage.PK_ITEM_IS_NOT_MAP_ERROR);
            }
        }
        return pks;
    }

    public static ColumnType parseColumnType(String type) {
        if (type.equalsIgnoreCase(TableStoreConst.TYPE_STRING)) {
            return ColumnType.STRING;
        } else if (type.equalsIgnoreCase(TableStoreConst.TYPE_INTEGER)) {
            return ColumnType.INTEGER;
        } else if (type.equalsIgnoreCase(TableStoreConst.TYPE_BOOLEAN)) {
            return ColumnType.BOOLEAN;
        } else if (type.equalsIgnoreCase(TableStoreConst.TYPE_DOUBLE)) {
            return ColumnType.DOUBLE;
        } else if (type.equalsIgnoreCase(TableStoreConst.TYPE_BINARY)) {
            return ColumnType.BINARY;
        } else {
            throw new IllegalArgumentException(String.format(TableStoreErrorMessage.ATTR_TYPE_ERROR, type));
        }
    }

    public static TableStoreAttrColumn parseTableStoreAttrColumn(Map<String, Object> column) {
        if (column.containsKey(TableStoreConst.NAME) && column.containsKey(TableStoreConst.TYPE)
                && column.containsKey(TableStoreConst.SEQUENCE) && column.size() == 3) {
            Object type = column.get(TableStoreConst.TYPE);
            Object name = column.get(TableStoreConst.NAME);
            Object sequence = column.get(TableStoreConst.SEQUENCE);
            if (type instanceof String && name instanceof String && sequence instanceof Integer) {
                String typeStr = (String) type;
                String nameStr = (String) name;
                Integer sequenceInt = (Integer) sequence;
                if (nameStr.isEmpty()) {
                    throw new IllegalArgumentException(TableStoreErrorMessage.ATTR_COLUMN_NAME_IS_EMPTY_ERROR);
                }
                return new TableStoreAttrColumn(nameStr, parseColumnType(typeStr), sequenceInt);
            } else {
                throw new IllegalArgumentException(TableStoreErrorMessage.ATTR_MAP_NAME_TYPE_ERROR);
            }
        } else {
            throw new IllegalArgumentException(TableStoreErrorMessage.ATTR_MAP_INCLUDE_NAME_TYPE_ERROR);
        }
    }

    private static void checkMultiAttrColumn(List<TableStoreAttrColumn> attrs) {
        Set<String> pool = new HashSet<String>();
        for (TableStoreAttrColumn col : attrs) {
            if (pool.contains(col.getName())) {
                throw new IllegalArgumentException(String.format(TableStoreErrorMessage.MULTI_ATTR_COLUMN_ERROR, col.getName()));
            } else {
                pool.add(col.getName());
            }
        }
    }

    public static List<TableStoreAttrColumn> parseTableStoreAttrColumnList(List<Object> values) {
        List<TableStoreAttrColumn> attrs = new ArrayList<TableStoreAttrColumn>();
        for (Object obj : values) {
            if (obj instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> column = (Map<String, Object>) obj;
                attrs.add(parseTableStoreAttrColumn(column));
            } else {
                throw new IllegalArgumentException(TableStoreErrorMessage.ATTR_ITEM_IS_NOT_MAP_ERROR);
            }
        }
        checkMultiAttrColumn(attrs);
        return attrs;
    }

    public static TableStoreOpType parseTableStoreOpType(String value) {
        if (value.equalsIgnoreCase(TableStoreConst.TABLE_STORE_OP_TYPE_PUT)) {
            return TableStoreOpType.PUT_ROW;
        } else if (value.equalsIgnoreCase(TableStoreConst.TABLE_STORE_OP_TYPE_UPDATE)) {
            return TableStoreOpType.UPDATE_ROW;
        } else if (value.equalsIgnoreCase(TableStoreConst.TABLE_STORE_OP_TYPE_DELETE)) {
            return TableStoreOpType.DELETE_ROW;
        } else {
            throw new IllegalArgumentException(String.format(TableStoreErrorMessage.OPERATION_PARSE_ERROR, value));
        }
    }
}
