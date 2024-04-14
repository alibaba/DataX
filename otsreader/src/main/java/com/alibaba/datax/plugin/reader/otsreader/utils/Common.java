package com.alibaba.datax.plugin.reader.otsreader.utils;

import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSCriticalException;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSPrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.timeseries.ScanTimeseriesDataResponse;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Common {
    public static List<String> toColumnToGet(List<OTSColumn> columns, TableMeta meta) {
        Map<String, PrimaryKeyType> pk = meta.getPrimaryKeyMap();
        List<String> names = new ArrayList<String>();
        for (OTSColumn c : columns) {
            if (c.getColumnType() == OTSColumn.OTSColumnType.NORMAL && !pk.containsKey(c.getName())) {
                names.add(c.getName());
            }
        }
        return names;
    }

    public static List<String> getPrimaryKeyNameList(TableMeta meta) {
        List<String> names = new ArrayList<String>();
        names.addAll(meta.getPrimaryKeyMap().keySet());
        return names;
    }

    public static OTSPrimaryKeyColumn getPartitionKey(TableMeta meta) {
        List<String> keys = new ArrayList<String>();
        keys.addAll(meta.getPrimaryKeyMap().keySet());

        String key = keys.get(0);

        OTSPrimaryKeyColumn col = new OTSPrimaryKeyColumn();
        col.setName(key);
        col.setType(meta.getPrimaryKeyMap().get(key));
        return col;
    }
    
    public static Direction getDirection(List<PrimaryKeyColumn> begin, List<PrimaryKeyColumn> end) throws OTSCriticalException {
        int cmp = CompareHelper.comparePrimaryKeyColumnList(begin, end);
        if (cmp < 0) {
            return Direction.FORWARD;
        } else if (cmp > 0) {
            return Direction.BACKWARD;
        } else {
            throw new OTSCriticalException("Bug branch, the begin of range equals end of range.");
        }
    }

    public static int compareRangeBeginAndEnd(TableMeta meta, List<PrimaryKeyColumn> begin, List<PrimaryKeyColumn> end) {
        if (begin.size() != end.size()) {
            throw new IllegalArgumentException("Input size of begin not equal size of end, begin size : " + begin.size() +
                    ", end size : " + end.size() + ".");
        }

        Map<String, PrimaryKeyValue> beginMap = new HashMap<>();
        Map<String, PrimaryKeyValue> endMap = new HashMap<>();

        for(PrimaryKeyColumn primaryKeyColumn : begin){
            beginMap.put(primaryKeyColumn.getName(), primaryKeyColumn.getValue());
        }
        for(PrimaryKeyColumn primaryKeyColumn : end){
            endMap.put(primaryKeyColumn.getName(), primaryKeyColumn.getValue());
        }

        for (String key : meta.getPrimaryKeyMap().keySet()) {
            PrimaryKeyValue v1 = beginMap.get(key);
            PrimaryKeyValue v2 = endMap.get(key);
            int cmp = primaryKeyValueCmp(v1, v2);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }


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

    public static void checkTableStoreSDKVersion() throws OTSCriticalException {
        Field[] fields = ScanTimeseriesDataResponse.class.getFields();
        String sdkVersion = null;
        for (Field f : fields){
            if (f.getName().equals("_VERSION_")){
                sdkVersion = ScanTimeseriesDataResponse._VERSION_;
                break;
            }
        }
        if (sdkVersion == null){
            throw new OTSCriticalException("Check ots java SDK failed. Please check the version of tableStore maven dependency.");
        }else if (Integer.parseInt(sdkVersion) < 20230111){
            throw new OTSCriticalException("Check tableStore java SDK failed. The expected version number is greater than 20230111, actually version : " + sdkVersion + ".");
        }
    }
}
