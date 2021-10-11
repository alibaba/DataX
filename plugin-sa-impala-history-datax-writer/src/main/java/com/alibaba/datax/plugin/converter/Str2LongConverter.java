package com.alibaba.datax.plugin.converter;

import com.alibaba.datax.plugin.Converter;
import com.alibaba.datax.plugin.util.NullUtil;

import java.util.Map;

public class Str2LongConverter  implements Converter {
    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param, Map<String,Object> resolvedValues) {
        if(NullUtil.isNullOrBlank(value)){
            return (Long) null;
        }
        if(value instanceof Long){
            return value;
        }
        return Long.parseLong( value.toString());
    }
}
