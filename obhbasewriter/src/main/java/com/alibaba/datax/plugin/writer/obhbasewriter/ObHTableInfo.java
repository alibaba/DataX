/*
 * Copyright (c) 2021 OceanBase ob-loader-dumper is licensed under Mulan PSL v2. You can use this software according to
 * the terms and conditions of the Mulan PSL v2. You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the Mulan PSL v2 for more
 * details.
 */
package com.alibaba.datax.plugin.writer.obhbasewriter;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Triple;

/**
 * @author cjyyz
 * @date 2023/03/24
 * @since
 */
public class ObHTableInfo {

    /**
     * 不带列族的表名，用于构建OHTable
     */
    String tableName;

    /**
     * 带列族的表名，用于分区计算
     */
    String fullHbaseTableName;

    NullModeType nullModeType;

    String encoding;

    List<Configuration> columns;

    /**
     * 记录配置文件中的columns的列族名，字段名，字段类型，避免每次执行插入都解析
     * Triple<String, String, String> left ： 列族名；middle ： 字段名；right：字段类型
     */
    LinkedHashMap<Integer, Triple<String, String, ColumnType>> indexColumnInfoMap;

    /**
     * 记录配置文件中rowKey的Index，常量值，字段类型，避免每次执行插入都解析
     * Triple<Integer, String, ColumnType> left ： Index；middle ： 常量值；right：字段类型
     */
    List<Triple<Integer, String, ColumnType>> rowKeyElementList;

    public ObHTableInfo(Configuration configuration) {
        this.nullModeType = NullModeType.getByTypeName(configuration.getString(ConfigKey.NULL_MODE, Constant.DEFAULT_NULL_MODE));
        this.encoding = configuration.getString(ConfigKey.ENCODING, Constant.DEFAULT_ENCODING);
        this.columns = configuration.getListConfiguration(ConfigKey.COLUMN);
        this.indexColumnInfoMap = new LinkedHashMap<>();
        configuration.getListConfiguration(ConfigKey.COLUMN).forEach(e -> {
            String[] name = e.getString(ConfigKey.NAME).split(":");
            indexColumnInfoMap.put(e.getInt(ConfigKey.INDEX), Triple.of(name[0], name[1], ColumnType.getByTypeName(e.getString(ConfigKey.TYPE)))
            );
        });

        this.rowKeyElementList = new ArrayList<>();
        configuration.getListConfiguration(ConfigKey.ROWKEY_COLUMN).forEach(e -> {
            Integer index = e.getInt(ConfigKey.INDEX);
            String constantValue = e.getString(ConfigKey.VALUE);
            ColumnType columnType = ColumnType.getByTypeName(e.getString(ConfigKey.TYPE));
            rowKeyElementList.add(Triple.of(index, constantValue, columnType));

        });

        this.tableName = configuration.getString(Key.TABLE);
        this.fullHbaseTableName = tableName;
        if (!fullHbaseTableName.contains("$")) {
            String name = columns.get(0).getString(ConfigKey.NAME);
            String familyName = name.split(":")[0];
            fullHbaseTableName = fullHbaseTableName + "$" + familyName;
        }
    }

    public String getTableName() {
        return tableName;
    }

    public String getFullHbaseTableName() {
        return fullHbaseTableName;
    }

    public NullModeType getNullModeType() {
        return nullModeType;
    }

    public String getEncoding() {
        return encoding;
    }

    public Map<Integer, Triple<String, String, ColumnType>> getIndexColumnInfoMap() {
        return indexColumnInfoMap;
    }

    public List<Triple<Integer, String, ColumnType>> getRowKeyElementList() {
        return rowKeyElementList;
    }
}