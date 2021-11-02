package com.alibaba.datax.plugin.converter;

import com.alibaba.datax.plugin.Converter;
import com.alibaba.datax.plugin.util.NullUtil;

import java.util.Map;

public class BytesArr2StrConverter implements Converter {


    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param, Map<String, Object> resolvedValues) {
        if(NullUtil.isNullOrBlank(value)){
            return (String) null;
        }
        if(value instanceof String){
            return value;
        }
        byte[] v = (byte[])value;
        if(NullUtil.isNullOrBlank(v) || v.length == 0){
            return null;
        }
        return new String(v);
    }
}
