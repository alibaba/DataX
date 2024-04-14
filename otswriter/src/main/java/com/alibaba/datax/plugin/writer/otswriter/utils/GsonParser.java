package com.alibaba.datax.plugin.writer.otswriter.utils;

import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alicloud.openservices.tablestore.model.Direction;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonParser {
    
    private static Gson gsonBuilder() {
        return new GsonBuilder()
        .create();
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
    
    public static String primaryKeyToJson (PrimaryKey row) {
        Gson g = gsonBuilder();
        return g.toJson(row);
    }
    
    public static String primaryKeySchemaToJson (PrimaryKeySchema schema) {
        Gson g = gsonBuilder();
        return g.toJson(schema);
    }

    public static PrimaryKeySchema jsonToPrimaryKeySchema (String jsonStr) {
        Gson g = gsonBuilder();
        return g.fromJson(jsonStr, PrimaryKeySchema.class);
    }
}
