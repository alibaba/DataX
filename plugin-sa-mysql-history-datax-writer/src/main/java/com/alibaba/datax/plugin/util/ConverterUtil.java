package com.alibaba.datax.plugin.util;


import com.alibaba.datax.plugin.Converter;
import com.alibaba.datax.plugin.domain.DataConverter;
import com.alibaba.datax.plugin.domain.SaColumnItem;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ConverterUtil {

    public static Object convert(String targetColumnName, Object value, SaColumnItem col, Map<String,Object> resolvedValues){
        List<DataConverter> dataConverters = col.getDataConverters();
        if(Objects.isNull(dataConverters) || dataConverters.isEmpty()){
            return value;
        }
        if(Objects.isNull(resolvedValues)){
            resolvedValues = new HashMap<>();
        }
        for (DataConverter dataConverter : dataConverters) {
            Converter converter = dataConverter.getConverter();
            if(Objects.isNull(converter)){
                continue;
            }
            value = converter.transform(targetColumnName,value,dataConverter.getParam(),resolvedValues);
        }
        return value;
    }
}
