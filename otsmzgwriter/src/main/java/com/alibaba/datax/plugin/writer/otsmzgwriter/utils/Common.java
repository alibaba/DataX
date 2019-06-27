package com.alibaba.datax.plugin.writer.otsmzgwriter.utils;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.otsmzgwriter.model.*;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.model.*;
import org.apache.commons.math3.util.Pair;

import java.lang.reflect.Constructor;
import java.util.*;

public class Common {

    public static String getDetailMessage(Exception exception) {
        if (exception instanceof OTSException) {
            OTSException e = (OTSException) exception;
            return "OTSException[ErrorCode:" + e.getErrorCode() + ", ErrorMessage:" + e.getMessage() + ", RequestId:" + e.getRequestId() + "]";
        } else if (exception instanceof ClientException) {
            ClientException e = (ClientException) exception;
            return "ClientException[ErrorCode:" + e.getErrorCode() + ", ErrorMessage:" + e.getMessage() + "]";
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
    public static RowPrimaryKey getPKFromRecord(OTSConf conf, List<Pair<String, ColumnValue>> attributes) {
        String tableLogicalName = conf.getTableLogicalName();
        List<OTSPKColumn> pkColumns = conf.getPrimaryKeyColumn();
        List<OTSAttrColumn> attributeColumn = conf.getAttributeColumn();
        String primaryKeyCombo = generatePrimaryKeyCombo(attributeColumn, attributes);
        String hashKey = generateHashKey(tableLogicalName, primaryKeyCombo);
        RowPrimaryKey primaryKey = new RowPrimaryKey();
        int pkCount = pkColumns.size();
        if (pkCount != 4) {
            throw new IllegalArgumentException(OTSErrorMessage.PK_COLUMN_LENGTH_ERROR);
        }
        // 获取属性列，并根据sequence排序，生成对应的
        primaryKey.addPrimaryKeyColumn("hash_key", PrimaryKeyValue.fromString(hashKey));
        primaryKey.addPrimaryKeyColumn("table_logical_name", PrimaryKeyValue.fromString(tableLogicalName));
        primaryKey.addPrimaryKeyColumn("primary_key_combo", PrimaryKeyValue.fromString(primaryKeyCombo));
        // FIXME 如果有问题，使用反射处理
        Class<?> cls;
        try {
            cls = Class.forName("com.aliyun.openservices.ots.model.PrimaryKeyValue");//通过反射实例化Person类对象。
            Constructor<?>[] declaredConstructors = cls.getDeclaredConstructors();
            for (int i = 0; i < declaredConstructors.length; i++) {
                Constructor constructor = declaredConstructors[i];
                Class[] parameterTypes = constructor.getParameterTypes();
                if (parameterTypes.length == 2
                        && parameterTypes[0].getName().equals("java.lang.Object")
                        && parameterTypes[1].getName().equals("com.aliyun.openservices.ots.model.PrimaryKeyType")) {
                    constructor.setAccessible(true);
                    PrimaryKeyValue autoIncrement = (PrimaryKeyValue) constructor.newInstance("AUTO_INCREMENT", null);
                    primaryKey.addPrimaryKeyColumn("serial_number", autoIncrement);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return primaryKey;
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
    private static String generatePrimaryKeyCombo(List<OTSAttrColumn> attrColumns, List<Pair<String, ColumnValue>> values) {
        Map<String, ColumnValue> valueMap = new HashMap<String, ColumnValue>();
        for (int i = 0; i < values.size(); i++) {
            valueMap.put(values.get(i).getKey(), values.get(i).getValue());
        }
        // 深度复制，防止修改原来列的排序
        List<OTSAttrColumn> targetList =
                BeanCopierUtils.mapList(attrColumns, OTSAttrColumn.class);
        Collections.sort(targetList);
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < targetList.size(); i++) {
            sb.append(valueMap.get(targetList.get(i).getName()).asString());
        }
        return sb.toString();
    }

    public static List<Pair<String, ColumnValue>> getAttrFromRecord(List<OTSAttrColumn> attrColumns, Record r) {
        List<Pair<String, ColumnValue>> attr = new ArrayList<Pair<String, ColumnValue>>(r.getColumnNumber());
        for (int i = 0; i < attrColumns.size(); i++) {
            Column col = r.getColumn(i);
            OTSAttrColumn expect = attrColumns.get(i);

            if (col.getRawData() == null) {
                attr.add(new Pair<String, ColumnValue>(expect.getName(), null));
                continue;
            }

            ColumnValue cv = ColumnConversion.columnToColumnValue(col, expect);
            attr.add(new Pair<String, ColumnValue>(expect.getName(), cv));
        }
        return attr;
    }

    public static RowChange columnValuesToRowChange(String tableName, OTSOpType type, RowPrimaryKey pk, List<Pair<String, ColumnValue>> values) {
        switch (type) {
            case PUT_ROW:
                RowPutChangeWithRecord rowPutChange = new RowPutChangeWithRecord(tableName);
                rowPutChange.setPrimaryKey(pk);

                for (Pair<String, ColumnValue> en : values) {
                    if (en.getValue() != null) {
                        rowPutChange.addAttributeColumn(en.getKey(), en.getValue());
                    }
                }

                return rowPutChange;
            case UPDATE_ROW:
                RowUpdateChangeWithRecord rowUpdateChange = new RowUpdateChangeWithRecord(tableName);
                rowUpdateChange.setPrimaryKey(pk);

                for (Pair<String, ColumnValue> en : values) {
                    if (en.getValue() != null) {
                        rowUpdateChange.addAttributeColumn(en.getKey(), en.getValue());
                    } else {
                        rowUpdateChange.deleteAttributeColumn(en.getKey());
                    }
                }
                return rowUpdateChange;
            case DELETE_ROW:
                RowDeleteChangeWithRecord rowDeleteChange = new RowDeleteChangeWithRecord(tableName);
                rowDeleteChange.setPrimaryKey(pk);
                return rowDeleteChange;
            default:
                throw new IllegalArgumentException(String.format(OTSErrorMessage.UNSUPPORT_PARSE, type, "RowChange"));
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
