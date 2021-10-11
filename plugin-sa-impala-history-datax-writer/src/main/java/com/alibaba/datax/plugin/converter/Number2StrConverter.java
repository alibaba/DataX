package com.alibaba.datax.plugin.converter;

import com.alibaba.datax.plugin.Converter;
import com.alibaba.datax.plugin.util.NullUtil;

import java.util.Map;

public class Number2StrConverter implements Converter {

    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param, Map<String,Object> resolvedValues) {
        if(NullUtil.isNullOrBlank(value)){
            return (String) null;
        }
        return value.toString();
    }
}
