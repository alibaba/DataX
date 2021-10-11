package com.alibaba.datax.plugin.converter;

import com.alibaba.datax.plugin.Converter;
import com.alibaba.datax.plugin.util.NullUtil;

import java.util.HashMap;
import java.util.Map;

public class StrEnumConverter implements Converter {

    private static final String ENUM_VALUE = "enum";
    private static final String DEFAULT_VALUE = "default";
    private static final String NULL_VALUE = "nullValue";

    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param, Map<String,Object> resolvedValues) {
        Object defaultValue = param.getOrDefault(DEFAULT_VALUE, null);
        Object nullValue = param.getOrDefault(NULL_VALUE, null);
        Map<String,Object> enumMap = (Map<String,Object>)param.getOrDefault(ENUM_VALUE, new HashMap<>());
        if(NullUtil.isNullOrBlank(value)){
            return nullValue;
        }
        return enumMap.getOrDefault(value.toString(), defaultValue);
    }
}
