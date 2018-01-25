package com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils;

import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alicloud.openservices.tablestore.model.StreamShard;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class GsonParser {

    public static String configToJson(OTSStreamReaderConfig config) {
        return new GsonBuilder().create().toJson(config);
    }

    public static OTSStreamReaderConfig jsonToConfig(String jsonStr) {
        return new GsonBuilder().create().fromJson(jsonStr, OTSStreamReaderConfig.class);
    }

    public static String listToJson(List<String> list) {
        return new GsonBuilder().create().toJson(list);
    }

    public static List<String> jsonToList(String jsonStr) {
        return new GsonBuilder().create().fromJson(jsonStr, new TypeToken<List<String>>(){}.getType());
    }

    public static Object toJson(List<StreamShard> allShards) {
        return new GsonBuilder().create().toJson(allShards);
    }

    public static List<StreamShard> fromJson(String jsonStr) {
        return new GsonBuilder().create().fromJson(jsonStr, new TypeToken<List<StreamShard>>(){}.getType());
    }
}
