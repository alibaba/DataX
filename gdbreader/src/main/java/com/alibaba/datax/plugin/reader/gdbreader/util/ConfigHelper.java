/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */
package com.alibaba.datax.plugin.reader.gdbreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.gdbreader.GdbReaderErrorCode;
import com.alibaba.datax.plugin.reader.gdbreader.Key;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * @author : Liu Jianping
 * @date : 2019/9/6
 */

public interface ConfigHelper {
    static void assertConfig(String key, Supplier<Boolean> f) {
        if (!f.get()) {
            throw DataXException.asDataXException(GdbReaderErrorCode.BAD_CONFIG_VALUE, key);
        }
    }

    static void assertHasContent(Configuration config, String key) {
        assertConfig(key, () -> StringUtils.isNotBlank(config.getString(key)));
    }

    static void assertGdbClient(Configuration config) {
        assertHasContent(config, Key.HOST);
        assertConfig(Key.PORT, () -> config.getInt(Key.PORT) > 0);

        assertHasContent(config, Key.USERNAME);
        assertHasContent(config, Key.PASSWORD);
    }

    static List<String> assertLabels(Configuration config) {
        Object labels = config.get(Key.LABEL);
        if (!(labels instanceof List)) {
            throw DataXException.asDataXException(GdbReaderErrorCode.BAD_CONFIG_VALUE, "labels should be List");
        }

        List<?> list = (List<?>) labels;
        List<String> configLabels = new ArrayList<>(0);
        list.forEach(n -> configLabels.add(String.valueOf(n)));

        return configLabels;
    }

    static List<Configuration> splitConfig(Configuration config, List<String> labels) {
        List<Configuration> configs = new ArrayList<>();
        for (String label : labels) {
            Configuration conf = config.clone();
            conf.set(Key.LABEL, label);

            configs.add(conf);
        }
        return configs;
    }

    static Configuration fromClasspath(String name) {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(name)) {
            return Configuration.from(is);
        } catch (IOException e) {
            throw new IllegalArgumentException("File not found: " + name);
        }
    }
}
