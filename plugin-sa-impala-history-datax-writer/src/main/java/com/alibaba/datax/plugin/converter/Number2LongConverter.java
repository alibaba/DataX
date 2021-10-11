package com.alibaba.datax.plugin.converter;

import com.alibaba.datax.plugin.Converter;
import com.alibaba.datax.plugin.util.NullUtil;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.Objects;

public class Number2LongConverter implements Converter {

    private static final String MODEL = "model";

    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param, Map<String,Object> resolvedValues) {
        if(NullUtil.isNullOrBlank(value)){
            return (Long) null;
        }
        if(value instanceof Long){
            return value;
        }
        BigDecimal v = new BigDecimal(value.toString());
//        四舍五入
        if(Objects.equals("half_up",param.getOrDefault(MODEL,""))){
            return v.setScale(0, RoundingMode.HALF_UP).longValue();
        }
//        向上取整
        if(Objects.equals("up",param.getOrDefault(MODEL,""))){
            return v.setScale(0, RoundingMode.UP).longValue();
        }
//       默认向下取整
        return v.setScale(0, RoundingMode.DOWN).longValue();
    }
}
