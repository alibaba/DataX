package com.alibaba.datax.plugin.converter;

import com.alibaba.datax.plugin.Converter;
import com.alibaba.datax.plugin.util.NullUtil;

import java.util.Date;
import java.util.Map;

public class Long2DateConverter implements Converter {

    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param, Map<String,Object> resolvedValues) {
        if(NullUtil.isNullOrBlank(value)){
            return (Date) null;
        }
        if(value instanceof Date){
            return value;
        }
        return new Date((Long) value);
    }
}
