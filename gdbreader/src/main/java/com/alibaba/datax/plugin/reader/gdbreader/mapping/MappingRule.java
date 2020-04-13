/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */
package com.alibaba.datax.plugin.reader.gdbreader.mapping;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.reader.gdbreader.GdbReaderErrorCode;
import com.alibaba.datax.plugin.reader.gdbreader.Key.ColumnType;
import com.alibaba.datax.plugin.reader.gdbreader.Key.ExportType;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * @author : Liu Jianping
 * @date : 2019/9/6
 */

@Data
public class MappingRule {
    private boolean hasRelation = false;
    private boolean hasProperty = false;
    private ExportType type = ExportType.VERTEX;

    /**
     * property names for property key-value
     */
    private List<String> propertyNames = new ArrayList<>();

    private List<ColumnMappingRule> columns = new ArrayList<>();

    void addColumn(ColumnType columnType, ValueType type, String name) {
        ColumnMappingRule rule = new ColumnMappingRule();
        rule.setColumnType(columnType);
        rule.setName(name);
        rule.setValueType(type);

        if (columnType == ColumnType.vertexProperty || columnType == ColumnType.edgeProperty) {
            propertyNames.add(name);
            hasProperty = true;
        }

        boolean hasTo = columnType == ColumnType.dstPrimaryKey || columnType == ColumnType.dstPrimaryLabel;
        boolean hasFrom = columnType == ColumnType.srcPrimaryKey || columnType == ColumnType.srcPrimaryLabel;
        if (hasTo || hasFrom) {
            hasRelation = true;
        }

        columns.add(rule);
    }

    void addJsonColumn(ColumnType columnType) {
        ColumnMappingRule rule = new ColumnMappingRule();
        rule.setColumnType(columnType);
        rule.setName("json");
        rule.setValueType(ValueType.STRING);

        if (!propertyNames.isEmpty()) {
            throw DataXException.asDataXException(GdbReaderErrorCode.BAD_CONFIG_VALUE, "JsonProperties should be only property");
        }

        columns.add(rule);
        hasProperty = true;
    }

    @Data
    protected static class ColumnMappingRule {
        private String name = null;

        private ValueType valueType = null;

        private ColumnType columnType = null;
    }
}
