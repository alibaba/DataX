package com.alibaba.datax.plugin.writer;

import com.alibaba.datax.common.util.Configuration;

import java.util.Properties;
import java.util.Set;

public class CommonUtil {

    public static Properties toProperties(Configuration configuration) {
        Set<String> keys = configuration.getKeys();
        Properties properties = new Properties();
        for (String key : keys) {
            String value = configuration.getString(key);
            if (value != null)
                properties.setProperty(key, value);
        }
        return properties;
    }
}
