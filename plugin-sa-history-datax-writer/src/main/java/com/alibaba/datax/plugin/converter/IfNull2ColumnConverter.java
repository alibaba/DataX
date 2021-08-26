package com.alibaba.datax.plugin.converter;

import com.alibaba.datax.plugin.Converter;
import com.alibaba.datax.plugin.util.NullUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Objects;

@Slf4j
public class IfNull2ColumnConverter implements Converter {

    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param, Map<String,Object> resolvedValues) {
        if(!NullUtil.isNullOrBlank(value)){
            return value;
        }
        String columnName = (String) param.get("targetColumnName");
        if(NullUtil.isNullOrBlank(columnName)){
            throw new RuntimeException("IfNull2Column转换器targetColumnName参数不能为空");
        }
        if(Objects.isNull(resolvedValues) || resolvedValues.isEmpty()){
            return null;
        }
        return resolvedValues.get(columnName);
    }
}
