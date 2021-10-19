/*
 * (C) 2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public
 * License version 2 as published by the Free Software Foundation.
 */
package com.alibaba.datax.plugin.writer.gdbwriter.mapping;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.gdbwriter.Key;
import com.alibaba.datax.plugin.writer.gdbwriter.client.GdbWriterConfig;

/**
 * @author : Liu Jianping
 * @date : 2019/10/15
 */

public class MapperConfig {
    private static MapperConfig instance = new MapperConfig();
    private int maxIdLength;
    private int maxLabelLength;
    private int maxPropKeyLength;
    private int maxPropValueLength;

    private MapperConfig() {
        this.maxIdLength = GdbWriterConfig.MAX_STRING_LENGTH;
        this.maxLabelLength = GdbWriterConfig.MAX_STRING_LENGTH;
        this.maxPropKeyLength = GdbWriterConfig.MAX_STRING_LENGTH;
        this.maxPropValueLength = GdbWriterConfig.MAX_STRING_LENGTH;
    }

    public static MapperConfig getInstance() {
        return instance;
    }

    public void updateConfig(final Configuration config) {
        final int length = config.getInt(Key.MAX_GDB_STRING_LENGTH, GdbWriterConfig.MAX_STRING_LENGTH);

        Integer sLength = config.getInt(Key.MAX_GDB_ID_LENGTH);
        this.maxIdLength = sLength == null ? length : sLength;

        sLength = config.getInt(Key.MAX_GDB_LABEL_LENGTH);
        this.maxLabelLength = sLength == null ? length : sLength;

        sLength = config.getInt(Key.MAX_GDB_PROP_KEY_LENGTH);
        this.maxPropKeyLength = sLength == null ? length : sLength;

        sLength = config.getInt(Key.MAX_GDB_PROP_VALUE_LENGTH);
        this.maxPropValueLength = sLength == null ? length : sLength;
    }

    public int getMaxIdLength() {
        return this.maxIdLength;
    }

    public int getMaxLabelLength() {
        return this.maxLabelLength;
    }

    public int getMaxPropKeyLength() {
        return this.maxPropKeyLength;
    }

    public int getMaxPropValueLength() {
        return this.maxPropValueLength;
    }

}
