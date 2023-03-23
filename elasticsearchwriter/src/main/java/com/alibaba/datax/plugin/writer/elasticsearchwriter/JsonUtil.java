package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONException;
import com.alibaba.fastjson2.JSONObject;

/**
 * @author bozu
 * @date 2021/01/06
 */
public class JsonUtil {

    /**
     * 合并两个json
     * @param source 源json
     * @param target 目标json
     * @return 合并后的json
     * @throws JSONException
     */
    public static String mergeJsonStr(String source, String target) throws JSONException {
        if(source == null) {
            return target;
        }
        if(target == null) {
            return source;
        }
        return JSON.toJSONString(deepMerge(JSON.parseObject(source), JSON.parseObject(target)));
    }

    /**
     * 深度合并两个json对象，将source的值，merge到target中
     * @param source 源json
     * @param target 目标json
     * @return 合并后的json
     * @throws JSONException
     */
    private static JSONObject deepMerge(JSONObject source, JSONObject target) throws JSONException {
        for (String key: source.keySet()) {
            Object value = source.get(key);
            if (target.containsKey(key)) {
                // existing value for "key" - recursively deep merge:
                if (value instanceof JSONObject) {
                    JSONObject valueJson = (JSONObject)value;
                    deepMerge(valueJson, target.getJSONObject(key));
                } else {
                    target.put(key, value);
                }
            } else {
                target.put(key, value);
            }
        }
        return target;
    }
}
