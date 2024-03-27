package com.alibaba.datax.common.util;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jitongchen
 * @date 2023/9/7 9:47 AM
 */
public class LimitLogger {

    private static Map<String, Long> lastPrintTime = new HashMap<>();

    public static void limit(String name, long limit, LoggerFunction function) {
        if (StringUtils.isBlank(name)) {
            name = "__all__";
        }
        if (limit <= 0) {
            function.apply();
        } else {
            if (!lastPrintTime.containsKey(name)) {
                lastPrintTime.put(name, System.currentTimeMillis());
                function.apply();
            } else {
                if (System.currentTimeMillis() > lastPrintTime.get(name) + limit) {
                    lastPrintTime.put(name, System.currentTimeMillis());
                    function.apply();
                }
            }
        }
    }
}
