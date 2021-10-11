package com.alibaba.datax.plugin.converter;

import com.alibaba.datax.plugin.Converter;

import java.util.Map;

public class NotNull2NullConverter implements Converter {

    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param, Map<String,Object> resolvedValues) {
        return null;
    }
}
