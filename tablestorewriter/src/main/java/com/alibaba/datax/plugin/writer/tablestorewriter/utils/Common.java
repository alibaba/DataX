package com.alibaba.datax.plugin.writer.tablestorewriter.utils;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.tablestorewriter.model.*;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;
import com.google.common.base.Strings;
import org.apache.commons.math3.util.Pair;

import java.util.*;

public class Common {

    public static String getDetailMessage(Exception exception) {
        if (exception instanceof TableStoreException) {
            TableStoreException e = (TableStoreException) exception;
            return "OTSException[ErrorCode:" + e.getErrorCode() + ", ErrorMessage:" + e.getMessage() + ", RequestId:" + e.getRequestId() + "]";
        } else if (exception instanceof ClientException) {
            ClientException e = (ClientException) exception;
            return "ClientException[TraceId:" + e.getTraceId() + ", ErrorMessage:" + e.getMessage() + "]";
        } else if (exception instanceof IllegalArgumentException) {
            IllegalArgumentException e = (IllegalArgumentException) exception;
            return "IllegalArgumentException[ErrorMessage:" + e.getMessage() + "]";
        } else {
            return "Exception[ErrorMessage:" + exception.getMessage() + "]";
        }
    }

    /**
     * primary key: hash_key,table_logical_name,primary_key_combo,serial_number
     * * 生成规则：
     * * 1、hash_key是table_logical_name和primary_key_combo两个字段的hash值
     * * 2、table_logical_name从job配置中获取
     * * 3、primary_key_combo是attribution column按照排序的顺序拼接而成
     * * 4、自增主键（table store提供实现）
     * *
     *
     * @param conf
     * @param attributes
     * @return
     */
    public static PrimaryKey getPKFromRecord(TableStoreConfig conf, List<Pair<String, ColumnValue>> attributes) {
        String tableLogicalName = conf.getTableLogicalName();
        List<TableStorePKColumn> pkColumns = conf.getPrimaryKeyColumn();
        List<TableStoreAttrColumn> attrColumn = conf.getAttrColumn();
        String primaryKeyCombo = generatePrimaryKeyCombo(attrColumn, attributes);
        String hashKey = generateHashKey(tableLogicalName, primaryKeyCombo);

        int pkCount = pkColumns.size();
        if (pkCount != 4) {
            throw new IllegalArgumentException(TableStoreErrorMessage.PK_COLUMN_LENGTH_ERROR);
        }
        // 获取属性列，并根据sequence排序，生成对应的
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("hash_key", PrimaryKeyValue.fromString(hashKey))
                .addPrimaryKeyColumn("table_logical_name", PrimaryKeyValue.fromString(tableLogicalName))
                .addPrimaryKeyColumn("primary_key_combo", PrimaryKeyValue.fromString(primaryKeyCombo))
                .addPrimaryKeyColumn("serial_number", PrimaryKeyValue.AUTO_INCREMENT);
        return primaryKeyBuilder.build();
    }

    private static String generateHashKey(String tableLogicalName, String primaryKeyCombo) {
        return CryptoUtils.crc32(tableLogicalName.concat(primaryKeyCombo));
    }

    /**
     * 按照attrColumns的sequence排序
     * 拼接所有字段，生成primary_key_combo
     *
     * @param attrColumns
     * @param values
     * @return
     */
    private static String generatePrimaryKeyCombo(List<TableStoreAttrColumn> attrColumns, List<Pair<String, ColumnValue>> values) {
        Map<String, ColumnValue> valueMap = new HashMap<String, ColumnValue>();
        for (int i = 0; i < values.size(); i++) {
            valueMap.put(values.get(i).getKey(), values.get(i).getValue());
        }
        // 深度复制，防止修改原来列的排序
        List<TableStoreAttrColumn> targetList =
                BeanCopierUtils.mapList(attrColumns, TableStoreAttrColumn.class);
        Collections.sort(targetList);
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < targetList.size(); i++) {
            String value = valueMap.get(targetList.get(i).getName()).asString();
            if (!Strings.isNullOrEmpty(value)) {
                sb.append(valueMap.get(targetList.get(i).getName()).asString());
            }
        }
        return sb.toString();
    }

    public static List<Pair<String, ColumnValue>> getAttrFromRecord(List<TableStoreAttrColumn> attrColumns, Record r) {
        List<Pair<String, ColumnValue>> attr = new ArrayList<Pair<String, ColumnValue>>(r.getColumnNumber());
        for (int i = 0; i < attrColumns.size(); i++) {
            Column col = r.getColumn(i);
            TableStoreAttrColumn expect = attrColumns.get(i);

            if (col.getRawData() == null) {
                attr.add(new Pair<String, ColumnValue>(expect.getName(), null));
                continue;
            }

            ColumnValue cv = ColumnConversion.columnToColumnValue(col, expect);
            attr.add(new Pair<String, ColumnValue>(expect.getName(), cv));
        }
        return attr;
    }

    public static RowChange columnValuesToRowChange(String tableName, TableStoreOpType type, PrimaryKey pk, List<Pair<String, ColumnValue>> values) {
        switch (type) {
            case PUT_ROW:
                RowPutChangeWithRecord rowPutChange = new RowPutChangeWithRecord(tableName);
                rowPutChange.setPrimaryKey(pk);

                for (Pair<String, ColumnValue> en : values) {
                    if (en.getValue() != null) {
                        rowPutChange.addColumn(en.getKey(), en.getValue());
                    }
                }

                return rowPutChange;
            case UPDATE_ROW:
                RowUpdateChangeWithRecord rowUpdateChange = new RowUpdateChangeWithRecord(tableName);
                rowUpdateChange.setPrimaryKey(pk);

                for (Pair<String, ColumnValue> en : values) {
                    if (en.getValue() != null) {
                        rowUpdateChange.put(en.getKey(), en.getValue());
                    } else {
                        rowUpdateChange.deleteColumns(en.getKey());
                    }
                }
                return rowUpdateChange;
            case DELETE_ROW:
                RowDeleteChangeWithRecord rowDeleteChange = new RowDeleteChangeWithRecord(tableName);
                rowDeleteChange.setPrimaryKey(pk);
                return rowDeleteChange;
            default:
                throw new IllegalArgumentException(String.format(TableStoreErrorMessage.UNSUPPORT_PARSE, type, "RowChange"));
        }
    }

    public static long getDelaySendMilliseconds(int hadRetryTimes, int initSleepInMilliSecond) {

        if (hadRetryTimes <= 0) {
            return 0;
        }

        int sleepTime = initSleepInMilliSecond;
        for (int i = 1; i < hadRetryTimes; i++) {
            sleepTime += sleepTime;
            if (sleepTime > 30000) {
                sleepTime = 30000;
                break;
            }
        }
        return sleepTime;
    }
}
