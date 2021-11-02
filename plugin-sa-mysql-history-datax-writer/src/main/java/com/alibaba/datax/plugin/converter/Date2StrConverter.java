package com.alibaba.datax.plugin.converter;

import cn.hutool.core.util.StrUtil;
import com.alibaba.datax.plugin.Converter;
import com.alibaba.datax.plugin.util.NullUtil;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class Date2StrConverter implements Converter {

    private static final String DEFAULT_PATTERN = "yyyy-MM-dd HH:mm:ss";

    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param, Map<String,Object> resolvedValues) {
        if(NullUtil.isNullOrBlank(value)){
            return (String) null;
        }
        if(value instanceof String){
            return value;
        }
        Date date = (Date) value;
        String pattern = (String)param.getOrDefault("pattern",DEFAULT_PATTERN);
        if(StrUtil.isBlank(pattern)){
            throw new RuntimeException("Date2Str转换器pattern参数不能为空");
        }
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(date);
    }
}
