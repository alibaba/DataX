package com.alibaba.datax.plugin.unstructuredstorage.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.ColumnEntry;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @Author: guxuan
 * @Date 2022-05-17 16:40
 */
public class ColumnTypeUtil {

    private static final String TYPE_NAME = "decimal";
    private static final String LEFT_BRACKETS = "(";
    private static final String RIGHT_BRACKETS = ")";
    private static final String DELIM = ",";

    public static boolean isDecimalType(String typeName){
        return typeName.toLowerCase().startsWith(TYPE_NAME);
    }

    public static DecimalInfo getDecimalInfo(String typeName, DecimalInfo defaultInfo){
        if(!isDecimalType(typeName)){
            throw new IllegalArgumentException("Unsupported column type:" + typeName);
        }

        if (typeName.contains(LEFT_BRACKETS) && typeName.contains(RIGHT_BRACKETS)){
            int precision = Integer.parseInt(typeName.substring(typeName.indexOf(LEFT_BRACKETS) + 1,typeName.indexOf(DELIM)).trim());
            int scale = Integer.parseInt(typeName.substring(typeName.indexOf(DELIM) + 1,typeName.indexOf(RIGHT_BRACKETS)).trim());
            return new DecimalInfo(precision, scale);
        } else {
            return defaultInfo;
        }
    }

    public static class DecimalInfo {
        private int precision;
        private int scale;

        public DecimalInfo(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
        }

        public int getPrecision() {
            return precision;
        }

        public int getScale() {
            return scale;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()){
                return false;

            }
            DecimalInfo that = (DecimalInfo) o;
            return precision == that.precision && scale == that.scale;
        }

        @Override
        public int hashCode() {
            return Objects.hash(precision, scale);
        }
    }

    public static List<ColumnEntry> getListColumnEntry(
            Configuration configuration, final String path) {
        List<JSONObject> lists = configuration.getList(path, JSONObject.class);
        if (lists == null) {
            return null;
        }
        List<ColumnEntry> result = new ArrayList<>();
        for (final JSONObject object : lists) {
            result.add(JSON.parseObject(object.toJSONString(), ColumnEntry.class));
        }
        return result;
    }
}
