package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import java.util.List;

import com.alibaba.fastjson2.JSONObject;

public class JsonPathUtil {

    public static JSONObject getJsonObject(List<String> paths, JSONObject data) {
        if (null == paths || paths.isEmpty()) {
            return data;
        }

        if (null == data) {
            return null;
        }

        JSONObject dataTmp = data;
        for (String each : paths) {
            if (null != dataTmp) {
                dataTmp = dataTmp.getJSONObject(each);
            } else {
                return null;
            }
        }
        return dataTmp;
    }
}