package com.alibaba.datax.plugin.reader.otsreader.utils;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.plugin.reader.otsreader.adaptor.ColumnAdaptor;
import com.alibaba.datax.plugin.reader.otsreader.adaptor.PrimaryKeyValueAdaptor;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesScanSplitInfo;
import com.aliyun.openservices.ots.model.Direction;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.Map;

public class GsonParser {
    
    private static Gson gsonBuilder() {
        return new GsonBuilder()
        .registerTypeAdapter(PrimaryKeyValue.class, new PrimaryKeyValueAdaptor())
        .registerTypeAdapter(Column.class, new ColumnAdaptor())
        .create();
    }

    public static String rangeToJson (OTSRange range) {
        Gson g = gsonBuilder();
        return g.toJson(range);
    }

    public static OTSRange jsonToRange (String jsonStr) {
        Gson g = gsonBuilder();
        return g.fromJson(jsonStr, OTSRange.class);
    }

    public static String confToJson (OTSConf conf) {
        Gson g = gsonBuilder();
        return g.toJson(conf);
    }

    public static OTSConf jsonToConf (String jsonStr) {
        Gson g = gsonBuilder();
        return g.fromJson(jsonStr, OTSConf.class);
    }
    
    public static String metaToJson (TableMeta meta) {
        Gson g = gsonBuilder();
        return g.toJson(meta);
    }

    public static TableMeta jsonToMeta (String jsonStr) {
        Gson g = gsonBuilder();
        return g.fromJson(jsonStr, TableMeta.class);
    }

    public static String timeseriesScanSplitInfoToString(TimeseriesScanSplitInfo timeseriesScanSplitInfo){
        Gson g = gsonBuilder();
        return g.toJson(timeseriesScanSplitInfo);
    }

    public static TimeseriesScanSplitInfo stringToTimeseriesScanSplitInfo(String jsonStr){
        Gson g = gsonBuilder();
        return g.fromJson(jsonStr, TimeseriesScanSplitInfo.class);
    }

    public static Direction jsonToDirection (String jsonStr) {
        Gson g = gsonBuilder();
        return g.fromJson(jsonStr, Direction.class);
    }

    public static String rowPrimaryKeyToJson (RowPrimaryKey row) {
        Gson g = gsonBuilder();
        return g.toJson(row);
    }

    public static String mapToJson (Map<String, String> map) {
        Gson g = gsonBuilder();
        return g.toJson(map);
    }
}
