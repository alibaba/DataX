package com.alibaba.datax.plugin.converter;

import com.alibaba.datax.plugin.Converter;
import com.alibaba.datax.plugin.util.NullUtil;

import java.util.Map;

public class Str2IntConverter implements Converter {
    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param, Map<String,Object> resolvedValues) {
        if(NullUtil.isNullOrBlank(value)){
            return (Integer) null;
        }
        if(value instanceof Integer){
            return value;
        }
        return Integer.parseInt(value.toString());
    }
}
