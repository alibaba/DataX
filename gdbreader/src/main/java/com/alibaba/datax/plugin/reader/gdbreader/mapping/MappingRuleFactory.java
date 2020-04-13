/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */
package com.alibaba.datax.plugin.reader.gdbreader.mapping;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.gdbreader.GdbReaderErrorCode;
import com.alibaba.datax.plugin.reader.gdbreader.Key;
import com.alibaba.datax.plugin.reader.gdbreader.Key.ColumnType;
import com.alibaba.datax.plugin.reader.gdbreader.Key.ExportType;
import com.alibaba.datax.plugin.reader.gdbreader.util.ConfigHelper;

import java.util.List;

/**
 * @author : Liu Jianping
 * @date : 2019/9/20
 */

public class MappingRuleFactory {
    private static final MappingRuleFactory instance = new MappingRuleFactory();

    public static MappingRuleFactory getInstance() {
        return instance;
    }

    public MappingRule create(Configuration config, ExportType exportType) {
        MappingRule rule = new MappingRule();

        rule.setType(exportType);
        List<Configuration> configurationList = config.getListConfiguration(Key.COLUMN);
        for (Configuration column : configurationList) {
            ColumnType columnType;
            try {
                columnType = ColumnType.valueOf(column.getString(Key.COLUMN_NODE_TYPE));
            } catch (NullPointerException | IllegalArgumentException e) {
                throw DataXException.asDataXException(GdbReaderErrorCode.BAD_CONFIG_VALUE, Key.COLUMN_NODE_TYPE);
            }

            if (exportType == ExportType.VERTEX) {
                // only id/label/property column allow when vertex
                ConfigHelper.assertConfig(Key.COLUMN_NODE_TYPE, () ->
                        columnType == ColumnType.primaryKey || columnType == ColumnType.primaryLabel
                                || columnType == ColumnType.vertexProperty || columnType == ColumnType.vertexJsonProperty);
            } else if (exportType == ExportType.EDGE) {
                // edge
                ConfigHelper.assertConfig(Key.COLUMN_NODE_TYPE, () ->
                        columnType == ColumnType.primaryKey || columnType == ColumnType.primaryLabel
                                || columnType == ColumnType.srcPrimaryKey || columnType == ColumnType.srcPrimaryLabel
                                || columnType == ColumnType.dstPrimaryKey || columnType == ColumnType.dstPrimaryLabel
                                || columnType == ColumnType.edgeProperty || columnType == ColumnType.edgeJsonProperty);
            }

            if (columnType == ColumnType.edgeProperty || columnType == ColumnType.vertexProperty) {
                String name = column.getString(Key.COLUMN_NAME);
                ValueType propType = ValueType.fromShortName(column.getString(Key.COLUMN_TYPE));

                ConfigHelper.assertConfig(Key.COLUMN_NAME, () -> name != null);
                if (propType == null) {
                    throw DataXException.asDataXException(GdbReaderErrorCode.UNSUPPORTED_TYPE, Key.COLUMN_TYPE);
                }
                rule.addColumn(columnType, propType, name);
            } else if (columnType == ColumnType.vertexJsonProperty || columnType == ColumnType.edgeJsonProperty) {
                rule.addJsonColumn(columnType);
            } else {
                rule.addColumn(columnType, ValueType.STRING, null);
            }
        }
        return rule;
    }
}
