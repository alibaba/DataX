package com.alibaba.datax.plugin.reader.tablestorereader.utils;

import com.alibaba.datax.plugin.reader.tablestorereader.adaptor.OTSColumnAdaptor;
import com.alibaba.datax.plugin.reader.tablestorereader.adaptor.PrimaryKeyValueAdaptor;
import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStoreColumn;
import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStoreConf;
import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStoreRange;
import com.alicloud.openservices.tablestore.model.Direction;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonParser {

    private static Gson gsonBuilder() {
        return new GsonBuilder()
                .registerTypeAdapter(TableStoreColumn.class, new OTSColumnAdaptor())
                .registerTypeAdapter(PrimaryKeyValue.class, new PrimaryKeyValueAdaptor())
                .create();
    }

    public static String rangeToJson(TableStoreRange range) {
        Gson g = gsonBuilder();
        return g.toJson(range);
    }

    public static TableStoreRange jsonToRange(String jsonStr) {
        Gson g = gsonBuilder();
        return g.fromJson(jsonStr, TableStoreRange.class);
    }

    public static String confToJson(TableStoreConf conf) {
        Gson g = gsonBuilder();
        return g.toJson(conf);
    }

    public static TableStoreConf jsonToConf(String jsonStr) {
        Gson g = gsonBuilder();
        return g.fromJson(jsonStr, TableStoreConf.class);
    }

    public static String directionToJson(Direction direction) {
        Gson g = gsonBuilder();
        return g.toJson(direction);
    }

    public static Direction jsonToDirection(String jsonStr) {
        Gson g = gsonBuilder();
        return g.fromJson(jsonStr, Direction.class);
    }

    public static String metaToJson(TableMeta meta) {
        Gson g = gsonBuilder();
        return g.toJson(meta);
    }

//    public static String rowPrimaryKeyToJson(RowPrimaryKey row) {
//        Gson g = gsonBuilder();
//        return g.toJson(row);
//    }
}
