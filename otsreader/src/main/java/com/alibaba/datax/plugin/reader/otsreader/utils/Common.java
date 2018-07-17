package com.alibaba.datax.plugin.reader.otsreader.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSPrimaryKeyColumn;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.Row;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.TableMeta;

public class Common {

    public static int primaryKeyValueCmp(PrimaryKeyValue v1, PrimaryKeyValue v2) {
        if (v1.getType() != null && v2.getType() != null) {
            if (v1.getType() != v2.getType()) {
                throw new IllegalArgumentException(
                        "Not same column type, column1:" + v1.getType() + ", column2:" + v2.getType());
            }
            switch (v1.getType()) {
            case INTEGER:
                Long l1 = Long.valueOf(v1.asLong());
                Long l2 = Long.valueOf(v2.asLong());
                return l1.compareTo(l2);
            case STRING:
                return v1.asString().compareTo(v2.asString());
            default:
                throw new IllegalArgumentException("Unsuporrt compare the type: " + v1.getType() + ".");
            }
        } else {
            if (v1 == v2) {
                return 0;
            } else {
                if (v1 == PrimaryKeyValue.INF_MIN) {
                    return -1;
                } else if (v1 == PrimaryKeyValue.INF_MAX) {
                    return 1;
                } 

                if (v2 == PrimaryKeyValue.INF_MAX) {
                    return -1;
                } else if (v2 == PrimaryKeyValue.INF_MIN) {
                    return 1;
                }    
            }
        }
        return 0;
    }
    
    public static OTSPrimaryKeyColumn getPartitionKey(TableMeta meta) {
        List<String> keys = new ArrayList<String>();
        keys.addAll(meta.getPrimaryKey().keySet());

        String key = keys.get(0);

        OTSPrimaryKeyColumn col = new OTSPrimaryKeyColumn();
        col.setName(key);
        col.setType(meta.getPrimaryKey().get(key));
        return col;
    }

    public static List<String> getPrimaryKeyNameList(TableMeta meta) {
        List<String> names = new ArrayList<String>();
        names.addAll(meta.getPrimaryKey().keySet());
        return names;
    }

    public static int compareRangeBeginAndEnd(TableMeta meta, RowPrimaryKey begin, RowPrimaryKey end) {
        if (begin.getPrimaryKey().size() != end.getPrimaryKey().size()) {
            throw new IllegalArgumentException("Input size of begin not equal size of end, begin size : " + begin.getPrimaryKey().size() + 
                    ", end size : " + end.getPrimaryKey().size() + ".");
        }
        for (String key : meta.getPrimaryKey().keySet()) {
            PrimaryKeyValue v1 = begin.getPrimaryKey().get(key);
            PrimaryKeyValue v2 = end.getPrimaryKey().get(key);
            int cmp = primaryKeyValueCmp(v1, v2);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    public static List<String> getNormalColumnNameList(List<OTSColumn> columns) {
        List<String> normalColumns = new ArrayList<String>();
        for (OTSColumn col : columns) {
            if (col.getColumnType() == OTSColumn.OTSColumnType.NORMAL) {
                normalColumns.add(col.getName());
            }
        }
        return normalColumns;
    }
    
    public static Record parseRowToLine(Row row, List<OTSColumn> columns, Record line) {
        Map<String, ColumnValue> values = row.getColumns();
        for (OTSColumn col : columns) {
            if (col.getColumnType() == OTSColumn.OTSColumnType.CONST) {
                line.addColumn(col.getValue());
            } else {
                ColumnValue v = values.get(col.getName());
                if (v == null) {
                    line.addColumn(new StringColumn(null));
                } else {
                    switch(v.getType()) {
                    case STRING:  line.addColumn(new StringColumn(v.asString())); break;
                    case INTEGER: line.addColumn(new LongColumn(v.asLong()));   break;
                    case DOUBLE:  line.addColumn(new DoubleColumn(v.asDouble())); break;
                    case BOOLEAN: line.addColumn(new BoolColumn(v.asBoolean()));  break;
                    case BINARY:  line.addColumn(new BytesColumn(v.asBinary()));  break;
                    default:
                        throw new IllegalArgumentException("Unsuporrt tranform the type: " + col.getValue().getType() + ".");
                    }
                }
            }
        }
        return line;
    }
    
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
    
    public static long getDelaySendMillinSeconds(int hadRetryTimes, int initSleepInMilliSecond) {

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
