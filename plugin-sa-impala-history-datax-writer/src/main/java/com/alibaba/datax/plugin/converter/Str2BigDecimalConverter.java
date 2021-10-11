package com.alibaba.datax.plugin.converter;

import com.alibaba.datax.plugin.Converter;
import com.alibaba.datax.plugin.util.NullUtil;

import java.math.BigDecimal;
import java.util.Map;

public class Str2BigDecimalConverter implements Converter {
    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param, Map<String,Object> resolvedValues) {
        if(NullUtil.isNullOrBlank(value)){
            return (BigDecimal) null;
        }
        if(value instanceof BigDecimal){
            return value;
        }
        return new BigDecimal(value.toString());
    }
}
