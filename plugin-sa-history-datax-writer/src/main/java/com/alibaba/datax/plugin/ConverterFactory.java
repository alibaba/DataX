package com.alibaba.datax.plugin;

import cn.hutool.core.util.StrUtil;
import com.alibaba.datax.plugin.converter.*;
import java.util.HashMap;
import java.util.Map;

public class ConverterFactory {

    private static Map<String,Converter> converterMap = new HashMap();

    static{
        converterMap.put("Long2Date",new Long2DateConverter());
        converterMap.put("Date2Str",new Date2StrConverter());
        converterMap.put("Date2Long",new Date2LongConverter());
        converterMap.put("Number2Str",new Number2StrConverter());
        converterMap.put("Str2Long",new Str2LongConverter());
        converterMap.put("Str2Date",new Str2DateConverter());
        converterMap.put("BigInt2Date",new BigInteger2DateConverter());
        converterMap.put("Str2Int",new Str2IntConverter());
        converterMap.put("Str2Double",new Str2DoubleConverter());
        converterMap.put("Str2BigDecimal",new Str2BigDecimalConverter());
        converterMap.put("IfNull2Default",new IfNull2DefaultConverter());
        converterMap.put("NotNull2Null",new NotNull2NullConverter());
        converterMap.put("IfElse",new IfElseConverter());
        converterMap.put("IfNull2Column",new IfNull2ColumnConverter());
    }

    public static Converter converter(String type){
        if(StrUtil.isEmpty(type)){
            return null;
        }
        return converterMap.get(type);
    }
}
