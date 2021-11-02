package com.alibaba.datax.plugin.converter;

import com.alibaba.datax.plugin.Converter;
import com.alibaba.datax.plugin.util.NullUtil;

import java.math.BigInteger;
import java.util.Date;
import java.util.Map;

public class BigInteger2DateConverter  implements Converter {
    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param, Map<String,Object> resolvedValues) {
        if(NullUtil.isNullOrBlank(value)){
            return (Date) null;
        }
        if(value instanceof Date){
            return value;
        }
        BigInteger v = (BigInteger) value;
        return new Date(v.longValue());
    }
}
