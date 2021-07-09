package cn.sensorsdata.datax.plugin.converter;

import cn.sensorsdata.datax.plugin.Converter;
import cn.sensorsdata.datax.plugin.util.NullUtil;

import java.util.Map;

public class Str2IntConverter implements Converter {
    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param) {
        if (NullUtil.isNullOrBlank(value)) {
            return (Integer) null;
        }
        return Integer.parseInt(value.toString());
    }
}
