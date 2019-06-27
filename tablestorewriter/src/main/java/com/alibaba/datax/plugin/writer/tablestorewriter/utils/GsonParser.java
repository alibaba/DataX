package com.alibaba.datax.plugin.writer.tablestorewriter.utils;

import com.alibaba.datax.plugin.writer.tablestorewriter.model.TableStoreConfig;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonParser {

    private static Gson gsonBuilder() {
        return new GsonBuilder().create();
    }

    public static String confToJson(TableStoreConfig conf) {
        Gson g = gsonBuilder();
        return g.toJson(conf);
    }

    public static TableStoreConfig jsonToConf(String jsonStr) {
        Gson g = gsonBuilder();
        return g.fromJson(jsonStr, TableStoreConfig.class);
    }

    public static String metaToJson(TableMeta meta) {
        Gson g = gsonBuilder();
        return g.toJson(meta);
    }
}
