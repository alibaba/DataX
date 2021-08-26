package com.alibaba.datax.plugin.converter;

import com.alibaba.datax.plugin.util.DateUtil;
import com.alibaba.datax.plugin.util.NullUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.datax.plugin.Converter;
import com.alibaba.fastjson.JSONArray;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Str2DateConverter implements Converter {

    private Set<String> formatsSet = new HashSet<>();

    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param, Map<String,Object> resolvedValues) {
        if(NullUtil.isNullOrBlank(value)){
            return (Date) null;
        }
        if(value instanceof Date){
            return value;
        }
        String pattern = null;
        try {
            pattern = (String) param.get("pattern");
        }catch (Exception e){}
        if (!formatsSet.contains(targetColumnName)) {
            JSONArray formatsJsonArray = null;
            try {
                formatsJsonArray = (JSONArray) param.get("formats");
            }catch (Exception e){}
            if(!Objects.isNull(formatsJsonArray)){
                formatsJsonArray.forEach(f->{
                    if (!DateUtil.hasFormatCustomize(f.toString())) {
                        DateUtil.registerFormat(f.toString());
                    }
                });
            }
            formatsSet.add(targetColumnName);
        }
        if(StrUtil.isBlank(pattern)){
            pattern = DateUtil.getPattern((String) value);
            if(StrUtil.isBlank(pattern)){
                Date date = DateUtil.str2DateCustomize((String) value);
                return date;
            }
        }
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        try {
            return sdf.parse(value.toString());
        } catch (ParseException e) {
            return DateUtil.str2Date((String)value);
        }
    }
}
