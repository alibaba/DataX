package com.alibaba.datax.plugin.reader.otsreader.utils;

import com.alibaba.datax.plugin.reader.otsreader.adaptor.OTSColumnAdaptor;
import com.alibaba.datax.plugin.reader.otsreader.adaptor.PrimaryKeyValueAdaptor;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.aliyun.openservices.ots.model.Direction;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.TableMeta;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonParser {
    
    private static Gson gsonBuilder() {
        return new GsonBuilder()
        .registerTypeAdapter(OTSColumn.class, new OTSColumnAdaptor())
        .registerTypeAdapter(PrimaryKeyValue.class, new PrimaryKeyValueAdaptor())
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

    public static String directionToJson (Direction direction) {
        Gson g = gsonBuilder();
        return g.toJson(direction);
    }

    public static Direction jsonToDirection (String jsonStr) {
        Gson g = gsonBuilder();
        return g.fromJson(jsonStr, Direction.class);
    }
    
    public static String metaToJson (TableMeta meta) {
        Gson g = gsonBuilder();
        return g.toJson(meta);
    }
    
    public static String rowPrimaryKeyToJson (RowPrimaryKey row) {
        Gson g = gsonBuilder();
        return g.toJson(row);
    }
}
