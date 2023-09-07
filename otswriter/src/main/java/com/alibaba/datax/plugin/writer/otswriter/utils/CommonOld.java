package com.alibaba.datax.plugin.writer.otswriter.utils;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSErrorMessage;
import com.alibaba.datax.plugin.writer.otswriter.model.RowDeleteChangeWithRecord;
import com.alibaba.datax.plugin.writer.otswriter.model.RowPutChangeWithRecord;
import com.alibaba.datax.plugin.writer.otswriter.model.RowUpdateChangeWithRecord;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RowChange;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import org.apache.commons.math3.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class CommonOld {

    public static RowPrimaryKey getPKFromRecord(List<PrimaryKeySchema> pkColumns, Record r) {
        RowPrimaryKey primaryKey = new RowPrimaryKey();
        int pkCount = pkColumns.size();
        for (int i = 0; i < pkCount; i++) {
            Column col = r.getColumn(i);
            PrimaryKeySchema expect = pkColumns.get(i);

            if (col.getRawData() == null) {
                throw new IllegalArgumentException(String.format(OTSErrorMessage.PK_COLUMN_VALUE_IS_NULL_ERROR, expect.getName()));
            }

            PrimaryKeyValue pk = ColumnConversionOld.columnToPrimaryKeyValue(col, expect);
            primaryKey.addPrimaryKeyColumn(expect.getName(), pk);
        }
        return primaryKey;
    }

    public static List<Pair<String, ColumnValue>> getAttrFromRecord(int pkCount, List<com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn> attrColumns, Record r) {
        List<Pair<String, ColumnValue>> attr = new ArrayList<Pair<String, ColumnValue>>(r.getColumnNumber());
        for (int i = 0; i < attrColumns.size(); i++) {
            Column col = r.getColumn(i + pkCount);
            com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn expect = attrColumns.get(i);

            if (col.getRawData() == null) {
                attr.add(new Pair<String, ColumnValue>(expect.getName(), null));
                continue;
            }

            ColumnValue cv = ColumnConversionOld.columnToColumnValue(col, expect);
            attr.add(new Pair<String, ColumnValue>(expect.getName(), cv));
        }
        return attr;
    }

    public static RowChange columnValuesToRowChange(String tableName,
                                                    com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType type,
                                                    RowPrimaryKey pk,
                                                    List<Pair<String, ColumnValue>> values) {
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
}
