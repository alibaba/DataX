package com.alibaba.datax.plugin.reader.tablestorereader.utils;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.tablestorereader.Key;
import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStoreColumn;
import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStoreConf;
import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStoreConst;
import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStorePrimaryKeyColumn;
import com.alibaba.datax.plugin.reader.tablestorereader.utils.query.QueryFactory;
import com.alibaba.fastjson.JSON;
import com.alicloud.openservices.tablestore.model.*;

import com.alicloud.openservices.tablestore.model.Column;
import org.checkerframework.checker.units.qual.K;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    public static TableStorePrimaryKeyColumn getPartitionKey(TableMeta meta) {
        List<String> keys = new ArrayList<String>();
        keys.addAll(meta.getPrimaryKeyMap().keySet());

        String key = keys.get(0);

        TableStorePrimaryKeyColumn col = new TableStorePrimaryKeyColumn();
        col.setName(key);
        col.setType(meta.getPrimaryKeyMap().get(key));
        return col;
    }

//    public static List<String> getPrimaryKeyNameList(TableMeta meta) {
//        List<String> names = new ArrayList<String>();
//        names.addAll(meta.getPrimaryKeyMap().keySet());
//        return names;
//    }
//
//    public static int compareRangeBeginAndEnd(TableMeta meta, RowPrimaryKey begin, RowPrimaryKey end) {
//        if (begin.getPrimaryKey().size() != end.getPrimaryKey().size()) {
//            throw new IllegalArgumentException("Input size of begin not equal size of end, begin size : " + begin.getPrimaryKey().size() +
//                    ", end size : " + end.getPrimaryKey().size() + ".");
//        }
//        for (String key : meta.getPrimaryKey().keySet()) {
//            PrimaryKeyValue v1 = begin.getPrimaryKey().get(key);
//            PrimaryKeyValue v2 = end.getPrimaryKey().get(key);
//            int cmp = primaryKeyValueCmp(v1, v2);
//            if (cmp != 0) {
//                return cmp;
//            }
//        }
//        return 0;
//    }

    public static List<String> getNormalColumnNameList(List<TableStoreColumn> columns) {
        List<String> normalColumns = new ArrayList<String>();
        for (TableStoreColumn col : columns) {
            if (col.getColumnType() == TableStoreColumn.OTSColumnType.NORMAL) {
                normalColumns.add(col.getName());
            }
        }
        return normalColumns;
    }

    public static Record parseRowToLine(Row row, List<String> columns, Record line) {
        for (String col : columns) {

            Column latestColumn = row.getLatestColumn(col);
            ColumnValue v = null;

            if (null != latestColumn) {
                v = latestColumn.getValue();
            }

            if (v == null) {
                line.addColumn(new StringColumn(null));
            } else {
                switch (v.getType()) {
                    case STRING:
                        line.addColumn(new StringColumn(v.asString()));
                        break;
                    case INTEGER:
                        line.addColumn(new LongColumn(v.asLong()));
                        break;
                    case DOUBLE:
                        line.addColumn(new DoubleColumn(v.asDouble()));
                        break;
                    case BOOLEAN:
                        line.addColumn(new BoolColumn(v.asBoolean()));
                        break;
                    case BINARY:
                        line.addColumn(new BytesColumn(v.asBinary()));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsuporrt tranform the type, column: " + col + ".");
                }
            }
        }
        return line;
    }

    public static String getDetailMessage(Exception exception) {
        return exception.getMessage();
//        if (exception instanceof OTSException) {
//            OTSException e = (OTSException) exception;
//            return "OTSException[ErrorCode:" + e.getErrorCode() + ", ErrorMessage:" + e.getMessage() + ", RequestId:" + e.getRequestId() + "]";
//        } else if (exception instanceof ClientException) {
//            ClientException e = (ClientException) exception;
//            return "ClientException[ErrorCode:" + e.getErrorCode() + ", ErrorMessage:" + e.getMessage() + "]";
//        } else if (exception instanceof IllegalArgumentException) {
//            IllegalArgumentException e = (IllegalArgumentException) exception;
//            return "IllegalArgumentException[ErrorMessage:" + e.getMessage() + "]";
//        } else {
//            return "Exception[ErrorMessage:" + exception.getMessage() + "]";
//        }
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

    public static TableStoreConf buildConf(Configuration param) throws Exception {
        TableStoreConf conf = new TableStoreConf();

        conf.setRetry(param.getInt(TableStoreConst.RETRY, 18));
        conf.setSleepInMilliSecond(param.getInt(TableStoreConst.SLEEP_IN_MILLI_SECOND, 100));

        // 必选参数
        conf.setEndpoint(ParamChecker.checkStringAndGet(param, Key.OTS_ENDPOINT));
        conf.setAccessId(ParamChecker.checkStringAndGet(param, Key.OTS_ACCESSID));
        conf.setAccesskey(ParamChecker.checkStringAndGet(param, Key.OTS_ACCESSKEY));
        conf.setInstanceName(ParamChecker.checkStringAndGet(param, Key.OTS_INSTANCE_NAME));
        conf.setTableName(ParamChecker.checkStringAndGet(param, Key.TABLE_NAME));
        conf.setIndexName(ParamChecker.checkStringAndGet(param, Key.INDEX_NAME));
        conf.setLimit(ParamChecker.checkIntegerAndGet(param, Key.LIMIT, 1000));
        conf.setColumnNames(ParamChecker.checkListAndGet(param, Key.COLUMN_NAME, true).stream()
                .map(Object::toString).collect(Collectors.toList()));

        QueryFactory.build(param.getConfiguration(Key.QUERY));

        conf.setQueryRow(JSON.toJSONString(param.get(Key.QUERY)));

        return conf;
    }
}
