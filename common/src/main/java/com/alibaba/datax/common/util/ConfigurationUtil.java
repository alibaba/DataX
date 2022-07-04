package com.alibaba.datax.common.util;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

public class ConfigurationUtil {
    private static final List<String> SENSITIVE_KEYS = Arrays.asList("password", "accessKey", "securityToken",
        "AccessKeyId", "AccessKeySecert", "AccessKeySecret", "clientPassword");

    public static Configuration filterSensitive(Configuration origin) {
        // shell 任务configuration metric 可能为null。
        if (origin == null) {
            return origin;
        }
        // 确保不影响入参的对象
        Configuration configuration = origin.clone();
        Set<String> keys = configuration.getKeys();
        for (final String key : keys) {
            boolean isSensitive = false;
            for (String sensitiveKey : SENSITIVE_KEYS) {
                if (StringUtils.endsWithIgnoreCase(key, sensitiveKey)) {
                    isSensitive = true;
                    break;
                }
            }

            if (isSensitive && configuration.get(key) instanceof String) {
                configuration.set(key, configuration.getString(key).replaceAll(".", "*"));
            }

        }
        return configuration;
    }
}