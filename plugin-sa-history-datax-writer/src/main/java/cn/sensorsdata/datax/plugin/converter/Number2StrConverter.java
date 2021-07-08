package cn.sensorsdata.datax.plugin.converter;

import cn.sensorsdata.datax.plugin.Converter;
import cn.sensorsdata.datax.plugin.util.NullUtil;

import java.util.Map;

public class Number2StrConverter implements Converter {

    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param) {
        if (NullUtil.isNullOrBlank(value)) {
            return (String) null;
        }
        return value.toString();
    }
}
